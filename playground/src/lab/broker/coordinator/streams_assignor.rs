//! The server-side task assignor of KIP-1071, after Kafka's
//! `StickyTaskAssignor`.
//!
//! A task is `(subtopology, partition)`. Every task gets one active copy and
//! every task of a stateful subtopology gets `num.standby.replicas` standby
//! copies, each on a process that holds no other copy of the task. An active
//! task stays with the member that runs it while that member is under the
//! active quota, `ceil(tasks / members)`; a task nobody keeps goes to a member
//! that held it as a standby, then to the least loaded process. The result
//! is deterministic: ties go to the smallest process id and member id.

use std::collections::{BTreeMap, BTreeSet};

use super::ids::MemberId;

/// A task: the subtopology and the partition.
pub type Task = (String, i32);

/// One member as the assignor sees it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AssignorMember {
    pub id: MemberId,
    /// The process the member runs in; load is balanced across processes.
    pub process_id: String,
    pub current_active: BTreeSet<Task>,
    pub current_standby: BTreeSet<Task>,
}

/// The task universe of one assignment.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct AssignorInput {
    pub tasks: BTreeSet<Task>,
    /// The subtopologies with a changelog.
    pub stateful: BTreeSet<String>,
    pub num_standby_replicas: i32,
}

/// The computed target: the active and standby tasks of every member that
/// has some.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct TaskAssignment {
    pub active: BTreeMap<MemberId, BTreeSet<Task>>,
    pub standby: BTreeMap<MemberId, BTreeSet<Task>>,
}

struct State<'a> {
    members: Vec<&'a AssignorMember>,
    processes: BTreeMap<&'a str, Vec<usize>>,
    active: Vec<BTreeSet<Task>>,
    standby: Vec<BTreeSet<Task>>,
}

impl State<'_> {
    fn load(&self, member: usize) -> usize {
        self.active[member].len() + self.standby[member].len()
    }

    fn process_load(&self, process: &str) -> usize {
        self.processes[process].iter().map(|m| self.load(*m)).sum()
    }

    fn process_holds(&self, process: &str, task: &Task) -> bool {
        self.processes[process]
            .iter()
            .any(|m| self.active[*m].contains(task) || self.standby[*m].contains(task))
    }

    /// The least loaded member of the least loaded process among the
    /// processes `eligible` accepts.
    fn least_loaded_member(&self, eligible: impl Fn(&str) -> bool) -> Option<usize> {
        let process = self
            .processes
            .keys()
            .copied()
            .filter(|p| eligible(p))
            .min_by_key(|p| (self.process_load(p), *p))?;
        self.processes[process]
            .iter()
            .copied()
            .min_by_key(|m| (self.load(*m), self.members[*m].id.clone()))
    }
}

/// Compute the target assignment.
#[must_use]
pub fn assign(members: &[AssignorMember], input: &AssignorInput) -> TaskAssignment {
    if members.is_empty() {
        return TaskAssignment::default();
    }
    let mut ordered: Vec<&AssignorMember> = members.iter().collect();
    ordered.sort_by(|a, b| a.id.cmp(&b.id));
    let mut processes: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, m) in ordered.iter().enumerate() {
        processes.entry(m.process_id.as_str()).or_default().push(i);
    }
    let n = ordered.len();
    let mut state = State {
        active: vec![BTreeSet::new(); n],
        standby: vec![BTreeSet::new(); n],
        members: ordered,
        processes,
    };
    assign_active(&mut state, input);
    if input.num_standby_replicas > 0 {
        assign_standby(&mut state, input);
    }
    let mut out = TaskAssignment::default();
    for (i, member) in state.members.iter().enumerate() {
        if !state.active[i].is_empty() {
            out.active
                .insert(member.id.clone(), state.active[i].clone());
        }
        if !state.standby[i].is_empty() {
            out.standby
                .insert(member.id.clone(), state.standby[i].clone());
        }
    }
    out
}

fn assign_active(state: &mut State<'_>, input: &AssignorInput) {
    let n = state.members.len();
    let quota = input.tasks.len().div_ceil(n);
    let prev_active: BTreeMap<&Task, usize> = state
        .members
        .iter()
        .enumerate()
        .flat_map(|(i, m)| m.current_active.iter().map(move |t| (t, i)))
        .fold(BTreeMap::new(), |mut acc, (t, i)| {
            acc.entry(t).or_insert(i);
            acc
        });
    let prev_standby: BTreeMap<&Task, Vec<usize>> = state
        .members
        .iter()
        .enumerate()
        .flat_map(|(i, m)| m.current_standby.iter().map(move |t| (t, i)))
        .fold(BTreeMap::new(), |mut acc, (t, i)| {
            acc.entry(t).or_default().push(i);
            acc
        });
    // The sticky passes go by partition first, so a range-like assignment
    // pairs the same partition of every subtopology.
    let mut remaining: Vec<&Task> = input.tasks.iter().collect();
    remaining.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    remaining.retain(|task| {
        let Some(&prev) = prev_active.get(task) else {
            return true;
        };
        if state.active[prev].len() >= quota {
            return true;
        }
        state.active[prev].insert((*task).clone());
        false
    });
    remaining.retain(|task| {
        let candidate = prev_standby
            .get(task)
            .into_iter()
            .flatten()
            .copied()
            .filter(|m| state.active[*m].len() < quota)
            .min_by_key(|m| (state.load(*m), state.members[*m].id.clone()));
        let Some(member) = candidate else {
            return true;
        };
        state.active[member].insert((*task).clone());
        false
    });
    remaining.sort();
    for task in remaining {
        if let Some(member) = state.least_loaded_member(|_| true) {
            state.active[member].insert(task.clone());
        }
    }
}

fn assign_standby(state: &mut State<'_>, input: &AssignorInput) {
    let stateful: Vec<&Task> = input
        .tasks
        .iter()
        .filter(|(subtopology, _)| input.stateful.contains(subtopology))
        .collect();
    let replicas = usize::try_from(input.num_standby_replicas).unwrap_or(0);
    let prev_standby: BTreeMap<&Task, Vec<usize>> = state
        .members
        .iter()
        .enumerate()
        .flat_map(|(i, m)| m.current_standby.iter().map(move |t| (t, i)))
        .fold(BTreeMap::new(), |mut acc, (t, i)| {
            acc.entry(t).or_default().push(i);
            acc
        });
    for task in stateful {
        for _ in 0..replicas {
            let sticky = prev_standby
                .get(task)
                .into_iter()
                .flatten()
                .copied()
                .filter(|m| !state.process_holds(&state.members[*m].process_id, task))
                .min_by_key(|m| (state.load(*m), state.members[*m].id.clone()));
            let member =
                sticky.or_else(|| state.least_loaded_member(|p| !state.process_holds(p, task)));
            let Some(member) = member else {
                // Not enough processes for another copy of this task.
                break;
            };
            state.standby[member].insert(task.clone());
        }
    }
}
