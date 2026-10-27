// Bridge-only selection; leave the website's existing suite selection unchanged.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { changedPaths } from './ci-changes.mjs';

export function bridgeInput(file) {
  return /^(kafkactl-lab\/|\.github\/workflows\/(kafkactl-lab|kafkactl-lab-ci)\.yml$|scripts\/(bridge-ci(?:\.test)?|ci-changes)\.mjs$|scripts\/snippets\/extract\.mjs$)/.test(file)
    || /^(go\.(mod|sum|work|work\.sum)|\.gitattributes)$/.test(file);
}

export function selectBridge(eventName, event, diff = changedPaths) {
  if (eventName === 'pull_request' && event.pull_request?.draft === true) {
    return { bridge: false, reason: 'draft: bridge tests and builds skipped' };
  }
  if (eventName === 'workflow_dispatch') return { bridge: true, reason: 'manual run' };
  try {
    let base, head;
    if (eventName === 'pull_request') {
      if (event.pull_request?.draft !== false) throw new Error('Missing PR metadata');
      base = event.pull_request.base?.sha;
      head = event.pull_request.head?.sha;
    } else if (eventName === 'merge_group') {
      base = event.merge_group?.base_sha;
      head = event.merge_group?.head_sha;
    } else if (eventName === 'push') {
      base = event.before;
      head = event.after;
    } else {
      throw new Error('Unknown event');
    }
    const bridge = diff(base, head).some(bridgeInput);
    return { bridge, reason: bridge ? 'bridge or shared Go/CI inputs changed' : 'no bridge inputs changed' };
  } catch (error) {
    return { bridge: true, reason: `full fallback: ${error.message}` };
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  let selected;
  try {
    selected = selectBridge(process.env.GITHUB_EVENT_NAME,
      JSON.parse(fs.readFileSync(process.env.GITHUB_EVENT_PATH, 'utf8')));
  } catch (error) {
    selected = { bridge: true, reason: `full fallback: ${error.message}` };
  }
  if (process.env.GITHUB_OUTPUT) fs.appendFileSync(process.env.GITHUB_OUTPUT, `bridge=${selected.bridge}\n`);
  console.log(JSON.stringify(selected, null, 2));
  if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY,
    `### Bridge CI selection\n\n${selected.reason.replace(/[\r\n]/g, ' ')}\n`);
}
