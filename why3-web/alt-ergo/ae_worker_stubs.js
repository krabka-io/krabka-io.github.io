// Unix primitives Alt-Ergo's solving loop touches for its wall-clock timer and
// profiling signals. A web worker has neither; the page enforces its own
// time limit by terminating the worker.

//Provides: caml_unix_setitimer
function caml_unix_setitimer(which, newv) { return newv; }
//Provides: caml_unix_getpid
function caml_unix_getpid() { return 1; }
//Provides: caml_unix_kill
function caml_unix_kill(pid, sig) { return 0; }
