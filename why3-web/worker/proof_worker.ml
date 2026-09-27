(* Why3 in a web worker, for the proof explorer's browser re-check.
 *
 * The page posts JSON strings; this worker answers with JSON strings. It loads
 * a Coma file that Creusot generated for one krabka-verified function, splits
 * it into the same verification conditions why3find proved, applies the same
 * transformations the recorded session applied, and prints each leaf task in
 * Alt-Ergo's native input language for the Alt-Ergo worker to discharge.
 *
 * Modelled on Why3's own src/trywhy3/why3_worker.ml (LGPL 2.1, Inria), with a
 * JSON protocol instead of OCaml marshalling so the page side stays plain
 * JavaScript. Everything it reads (why3.conf, the driver, Why3's stdlib and
 * Creusot's prelude) is embedded in the JavaScript by js_of_ocaml's pseudo
 * filesystem at link time.
 *
 * Protocol (one JSON object per message):
 *   {"cmd":"ping"}
 *     -> {"kind":"pong","why3":"1.8.2","prover":"Alt-Ergo 2.6.2"}
 *   {"cmd":"load","name":"majority_size","content":"<coma source>"}
 *     -> {"kind":"loaded","name":..., "theories":[{"name":"Coma","goals":[{"id":1,"name":"vc_majority_size","expl":"..."}]}]}
 *   {"cmd":"transform","id":1,"name":"split_vc"}
 *     -> {"kind":"children","id":1,"name":"split_vc","children":[{"id":2,"expl":"..."},...]}
 *   {"cmd":"task","id":2}
 *     -> {"kind":"task","id":2,"expl":"...","text":"<prover input>","pretty":"<sequent>"}
 *   any failure -> {"kind":"error","cmd":...,"id":...,"message":"..."} *)

open Why3
open Js_of_ocaml

let conf_file = "/why3.conf"
let input_file = "/session/input.coma"

let config : Whyconf.config = Whyconf.read_config (Some conf_file)
let main : Whyconf.main = Whyconf.get_main config

let prover : Whyconf.config_prover =
  let provers = Whyconf.get_provers config in
  if Whyconf.Mprover.is_empty provers then failwith "why3.conf names no prover";
  snd (Whyconf.Mprover.choose provers)

let env : Env.env = Env.create_env (Whyconf.loadpath main)

(* The Coma files carry the Rust source spans Creusot recorded on the proof
 * machine. Those files are not in the browser, and Why3 would otherwise log a
 * warning per span; the page links spans to GitHub itself. *)
let () = Loc.set_warning_hook (fun ?loc:_ _ -> ())

let driver : Driver.driver = Driver.load_driver_for_prover main env prover

(* ---- task registry ------------------------------------------------------------- *)

type entry = { task : Task.task; expl : string; name : string }

let entries : (int, entry) Hashtbl.t = Hashtbl.create 64
let next_id = ref 0

let register ~name task =
  incr next_id;
  let id = !next_id in
  let _, expl, _ = Termcode.goal_expl_task ~root:false task in
  Hashtbl.replace entries id { task; expl; name };
  id

let lookup id =
  match Hashtbl.find_opt entries id with
  | Some e -> e
  | None -> failwith (Printf.sprintf "unknown task id %d" id)

let goal_name task =
  match Task.task_goal task with
  | pr -> pr.Decl.pr_name.Ident.id_string
  | exception _ -> "goal"

(* ---- JSON helpers ---------------------------------------------------------------- *)

let send (json : Yojson.Safe.t) = Worker.post_message (Js.string (Yojson.Safe.to_string json))

let member name (json : Yojson.Safe.t) =
  match json with
  | `Assoc fields -> (try List.assoc name fields with Not_found -> `Null)
  | _ -> `Null

let string_member name json =
  match member name json with `String s -> s | _ -> ""

let int_member name json =
  match member name json with `Int i -> i | `Intlit s -> int_of_string s | _ -> -1

(* ---- commands ------------------------------------------------------------------- *)

let () = Sys_js.create_file ~name:input_file ~content:""

let load name content =
  Hashtbl.reset entries;
  next_id := 0;
  let ch = open_out input_file in
  output_string ch content;
  close_out ch;
  let theories, _ = Env.read_file ~format:"coma" Env.base_language env input_file in
  let theories =
    Wstdlib.Mstr.fold
      (fun th_name th acc ->
        let tasks = Task.split_theory th None None in
        let goals =
          List.map
            (fun task ->
              let name = goal_name task in
              let id = register ~name task in
              let e = lookup id in
              `Assoc [ ("id", `Int id); ("name", `String name); ("expl", `String e.expl) ])
            tasks
        in
        `Assoc [ ("name", `String th_name); ("goals", `List goals) ] :: acc)
      theories []
  in
  send (`Assoc [ ("kind", `String "loaded"); ("name", `String name); ("theories", `List (List.rev theories)) ])

let apply_transform name task =
  match Trans.lookup_trans env name with
  | Trans.Trans_one t -> [ Trans.apply t task ]
  | Trans.Trans_list t -> Trans.apply t task
  | Trans.Trans_with_args _ | Trans.Trans_with_args_l _ ->
      failwith (Printf.sprintf "transformation %s needs arguments" name)

let transform id name =
  let e = lookup id in
  let children =
    List.map
      (fun task ->
        let cid = register ~name:e.name task in
        let c = lookup cid in
        `Assoc [ ("id", `Int cid); ("expl", `String c.expl) ])
      (apply_transform name e.task)
  in
  send (`Assoc [ ("kind", `String "children"); ("id", `Int id); ("name", `String name); ("children", `List children) ])

let task id =
  let e = lookup id in
  let text = Format.asprintf "%a" (Driver.print_task driver) e.task in
  let pretty = Pp.string_of Pretty.print_sequent e.task in
  send
    (`Assoc
      [ ("kind", `String "task"); ("id", `Int id); ("name", `String e.name); ("expl", `String e.expl); ("text", `String text); ("pretty", `String pretty) ])

let ping () =
  let p = prover.Whyconf.prover in
  send
    (`Assoc
      [ ("kind", `String "pong");
        ("why3", `String Config.version);
        ("prover", `String (Printf.sprintf "%s %s" p.Whyconf.prover_name p.Whyconf.prover_version)) ])

let describe_exn e =
  match e with
  | Loc.Located (loc, e') ->
      Pp.sprintf "%a: %a" Loc.pp_position loc Exn_printer.exn_printer e'
  | e -> Pp.sprintf "%a" Exn_printer.exn_printer e

let handle (json : Yojson.Safe.t) =
  let cmd = string_member "cmd" json in
  let id = int_member "id" json in
  try
    match cmd with
    | "ping" -> ping ()
    | "load" -> load (string_member "name" json) (string_member "content" json)
    | "transform" -> transform id (string_member "name" json)
    | "task" -> task id
    | other -> failwith (Printf.sprintf "unknown command %S" other)
  with e ->
    send
      (`Assoc
        [ ("kind", `String "error"); ("cmd", `String cmd); ("id", `Int id); ("message", `String (describe_exn e)) ])

let () =
  Worker.set_onmessage (fun (data : Js.js_string Js.t) ->
      let text = Js.to_string data in
      match Yojson.Safe.from_string text with
      | json -> handle json
      | exception e ->
          send (`Assoc [ ("kind", `String "error"); ("message", `String ("bad message: " ^ Printexc.to_string e)) ]))
