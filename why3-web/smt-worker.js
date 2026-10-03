// Z3 and cvc5 use the same JSON protocol as the Alt-Ergo worker.
// Each worker handles one leaf: cvc5's CLI exits, and Z3 owns pthread workers.
const workerUrl = self.proofWorkerUrl || self.location.href;
const prover = new URL(workerUrl).searchParams.get('prover');
const asset = (name) => new URL(name, workerUrl).href;

self.onmessage = async ({ data }) => {
  const request = JSON.parse(data);
  const started = performance.now();
  let output = '';
  let diagnostic = '';
  let replied = false;
  const finish = (exitCode = 0) => {
    if (replied) return;
    replied = true;
    const answers = output.trim().split(/\r?\n/).filter((line) => /^(unsat|sat|unknown)$/.test(line));
    // A diagnostic containing 'unsat' is never a proof. Reject partial answers
    // followed by parser errors, multiple queries, and nonzero CLI exits.
    const status = exitCode === 0 && !diagnostic && !/\(error\b/.test(output) && answers.length === 1 ? answers[0] : 'error';
    postMessage(JSON.stringify({ id: request.id, status, output, diagnostic, ms: performance.now() - started }));
  };
  try {
    if (prover === 'z3') {
      if (!self.crossOriginIsolated) throw new Error('Z3 needs cross-origin isolation for shared memory.');
      self.exports = {};
      importScripts(asset('z3-built.js'), asset('z3-api.js'));
      const { Z3 } = await exports.init(() => initZ3({
        locateFile: asset,
        // Pthreads also inherit isolation through a blob, like the main worker.
        mainScriptUrlOrBlob: new Blob([`importScripts(${JSON.stringify(asset('z3-built.js'))});`], { type: 'text/javascript' }),
        printErr: (text) => { diagnostic += text + '\n'; },
      }));
      const config = Z3.mk_config();
      const context = Z3.mk_context(config);
      Z3.del_config(config);
      try {
        output = await Z3.eval_smtlib2_string(context, request.content);
        const code = Z3.get_error_code(context);
        if (code) diagnostic += Z3.get_error_msg(context, code);
      } finally {
        Z3.del_context(context);
      }
      finish();
    } else if (prover === 'cvc5') {
      // The pinned non-modular CLI exposes FS/callMain as worker globals.
      self.Module = {
        locateFile: asset,
        print: (text) => { output += text + '\n'; },
        printErr: (text) => {
          // Emscripten warns about native resource-reporting syscalls. These
          // do not affect the answer; the page enforces its own time budget.
          if (!/^warning: unsupported syscall: __syscall_(prlimit64|getrusage)\s*$/.test(text)) diagnostic += text + '\n';
        },
        onAbort: (text) => { diagnostic += String(text); finish(1); },
        preRun: [() => {
          // Why3 asks for the unknown reason after check-sat. This diagnostic
          // query is invalid after sat/unsat in cvc5; it does not affect solving.
          FS.writeFile('/task.smt2', request.content.replace(/^\(get-info :reason-unknown\)\s*$/gm, ''));
        }],
        onRuntimeInitialized: () => {
          shouldRunNow = false;
          try {
            finish(callMain(['--lang=smt2', '/task.smt2']));
          } catch (error) {
            diagnostic += error.message || String(error);
            finish(1);
          }
        },
      };
      importScripts(asset('cvc5.js'));
    } else {
      throw new Error(`Unsupported prover ${prover}`);
    }
  } catch (error) {
    diagnostic += error.message || String(error);
    finish(1);
  }
};
