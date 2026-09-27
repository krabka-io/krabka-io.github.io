(* Alt-Ergo as a web worker, driven through the same solving loop as the
   command-line binary, so the Dolmen front end and SMT-LIB input work.
   Message in: JSON {"id":n,"filename":"task.smt2","content":"...","steps":n}
   Message out: JSON {"id":n,"status":"unsat"|"sat"|"unknown"|"error"|"timeout",
                      "output":"...","diagnostic":"...","ms":n} *)
open Alt_ergo_common
open AltErgoLib
open Js_of_ocaml

let json_string s =
  let b = Buffer.create (String.length s + 2) in
  Buffer.add_char b '"';
  String.iter
    (fun c ->
      match c with
      | '"' -> Buffer.add_string b "\\\""
      | '\\' -> Buffer.add_string b "\\\\"
      | '\n' -> Buffer.add_string b "\\n"
      | '\r' -> Buffer.add_string b "\\r"
      | '\t' -> Buffer.add_string b "\\t"
      | c when Char.code c < 0x20 -> Buffer.add_string b (Printf.sprintf "\\u%04x" (Char.code c))
      | c -> Buffer.add_char b c)
    s;
  Buffer.add_char b '"';
  Buffer.contents b

let field name json =
  (* minimal extraction: the page sends flat objects with string/number values *)
  let re = Str.regexp ("\"" ^ name ^ "\"[ ]*:[ ]*") in
  match Str.search_forward re json 0 with
  | exception Not_found -> None
  | _ ->
    let start = Str.match_end () in
    if start < String.length json && json.[start] = '"' then begin
      (* decode a JSON string *)
      let b = Buffer.create 256 in
      let i = ref (start + 1) in
      let n = String.length json in
      let fin = ref false in
      while not !fin && !i < n do
        (match json.[!i] with
         | '"' -> fin := true
         | '\\' ->
           incr i;
           (match json.[!i] with
            | 'n' -> Buffer.add_char b '\n'
            | 'r' -> Buffer.add_char b '\r'
            | 't' -> Buffer.add_char b '\t'
            | 'u' ->
              let code = int_of_string ("0x" ^ String.sub json (!i + 1) 4) in
              i := !i + 4;
              if code < 128 then Buffer.add_char b (Char.chr code) else Buffer.add_string b "?"
            | c -> Buffer.add_char b c)
         | c -> Buffer.add_char b c);
        incr i
      done;
      Some (Buffer.contents b)
    end else begin
      let stop = ref start in
      let n = String.length json in
      while !stop < n && (match json.[!stop] with '0'..'9' | '-' | '.' -> true | _ -> false) do incr stop done;
      Some (String.sub json start (!stop - start))
    end

let () = Input_frontend.register_legacy ()
let () = Dolmen_loop.Code.init []

let filename = "/task.smt2"
let () = Sys_js.create_file ~name:filename ~content:""

let run id content steps =
  let ch = open_out filename in
  output_string ch content;
  close_out ch;
  let regular = Buffer.create 1024 and diagnostic = Buffer.create 1024 in
  let fmt_r = Format.formatter_of_buffer regular and fmt_d = Format.formatter_of_buffer diagnostic in
  Options.Output.set_regular (Options.Output.of_formatter fmt_r);
  Options.Output.set_diagnostic (Options.Output.of_formatter fmt_d);
  Options.set_file_for_js filename;
  (* Why3's Alt-Ergo drivers emit polymorphic declarations (`par`), the
     dialect Alt-Ergo reads from .psmt2 files. *)
  Options.set_input_format (Some (Options.Smtlib2 `Poly));
  Options.set_steps_bound steps;
  Options.set_timelimit_per_goal false;
  Options.set_answers_with_loc false;
  let t0 = Sys.time () in
  let status =
    match Solving_loop.main () with
    | () -> None
    | exception e -> Some (Printexc.to_string e)
  in
  Format.pp_print_flush fmt_r ();
  Format.pp_print_flush fmt_d ();
  let out = Buffer.contents regular and diag = Buffer.contents diagnostic in
  let has s = Str.string_match (Str.regexp_string s) out 0 || (try ignore (Str.search_forward (Str.regexp_string s) out 0); true with Not_found -> false) in
  let verdict =
    if has "unsat" then "unsat"
    else if has "timeout" || has "Timeout" then "timeout"
    else if has "sat" then "sat"
    else if has "unknown" then "unknown"
    else "error"
  in
  let ms = int_of_float ((Sys.time () -. t0) *. 1000.) in
  Printf.sprintf "{\"id\":%d,\"status\":%s,\"output\":%s,\"diagnostic\":%s,\"exception\":%s,\"ms\":%d}"
    id (json_string verdict) (json_string out) (json_string diag)
    (json_string (Option.value status ~default:"")) ms

let () =
  Worker.set_onmessage (fun (data : Js.js_string Js.t) ->
      let msg = Js.to_string data in
      let id = match field "id" msg with Some s -> (try int_of_string s with _ -> 0) | None -> 0 in
      let content = Option.value (field "content" msg) ~default:"" in
      let steps = match field "steps" msg with Some s -> (try int_of_string s with _ -> -1) | None -> -1 in
      let reply =
        try run id content steps
        with e -> Printf.sprintf "{\"id\":%d,\"status\":\"error\",\"exception\":%s}" id (json_string (Printexc.to_string e))
      in
      Worker.post_message (Js.string reply))
