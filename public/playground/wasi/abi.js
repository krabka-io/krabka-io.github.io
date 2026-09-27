// WASI preview 1 (`wasi_snapshot_preview1`) ABI constants: the values and
// struct offsets the worker's shim reads and writes. Numbers follow the
// preview-1 witx definitions; wasi-libc uses the same errno numbering.

export const ERRNO = Object.freeze({
  SUCCESS: 0, "2BIG": 1, ACCES: 2, ADDRINUSE: 3, ADDRNOTAVAIL: 4, AFNOSUPPORT: 5, AGAIN: 6, ALREADY: 7,
  BADF: 8, BADMSG: 9, BUSY: 10, CANCELED: 11, CHILD: 12, CONNABORTED: 13, CONNREFUSED: 14, CONNRESET: 15,
  DEADLK: 16, DESTADDRREQ: 17, DOM: 18, DQUOT: 19, EXIST: 20, FAULT: 21, FBIG: 22, HOSTUNREACH: 23,
  IDRM: 24, ILSEQ: 25, INPROGRESS: 26, INTR: 27, INVAL: 28, IO: 29, ISCONN: 30, ISDIR: 31, LOOP: 32,
  MFILE: 33, MLINK: 34, MSGSIZE: 35, MULTIHOP: 36, NAMETOOLONG: 37, NETDOWN: 38, NETRESET: 39,
  NETUNREACH: 40, NFILE: 41, NOBUFS: 42, NODEV: 43, NOENT: 44, NOEXEC: 45, NOLCK: 46, NOLINK: 47,
  NOMEM: 48, NOMSG: 49, NOPROTOOPT: 50, NOSPC: 51, NOSYS: 52, NOTCONN: 53, NOTDIR: 54, NOTEMPTY: 55,
  NOTRECOVERABLE: 56, NOTSOCK: 57, NOTSUP: 58, NOTTY: 59, NXIO: 60, OVERFLOW: 61, OWNERDEAD: 62,
  PERM: 63, PIPE: 64, PROTO: 65, PROTONOSUPPORT: 66, PROTOTYPE: 67, RANGE: 68, ROFS: 69, SPIPE: 70,
  SRCH: 71, STALE: 72, TIMEDOUT: 73, TXTBSY: 74, XDEV: 75, NOTCAPABLE: 76,
});

export const FILETYPE = Object.freeze({
  UNKNOWN: 0, BLOCK_DEVICE: 1, CHARACTER_DEVICE: 2, DIRECTORY: 3, REGULAR_FILE: 4,
  SOCKET_DGRAM: 5, SOCKET_STREAM: 6, SYMBOLIC_LINK: 7,
});

export const FDFLAGS = Object.freeze({ APPEND: 1, DSYNC: 2, NONBLOCK: 4, RSYNC: 8, SYNC: 16 });
export const OFLAGS = Object.freeze({ CREAT: 1, DIRECTORY: 2, EXCL: 4, TRUNC: 8 });
export const FSTFLAGS = Object.freeze({ ATIM: 1, ATIM_NOW: 2, MTIM: 4, MTIM_NOW: 8 });
export const WHENCE = Object.freeze({ SET: 0, CUR: 1, END: 2 });
export const CLOCKID = Object.freeze({ REALTIME: 0, MONOTONIC: 1, PROCESS_CPUTIME_ID: 2, THREAD_CPUTIME_ID: 3 });
export const EVENTTYPE = Object.freeze({ CLOCK: 0, FD_READ: 1, FD_WRITE: 2 });
export const EVENTRWFLAGS_HANGUP = 1;
export const SUBCLOCKFLAGS_ABSTIME = 1;
export const RIFLAGS = Object.freeze({ PEEK: 1, WAITALL: 2 });
export const SDFLAGS = Object.freeze({ RD: 1, WR: 2 });
export const PREOPENTYPE_DIR = 0;
export const ADVICE_MAX = 5;

// Rights (u64 bit flags; the shim keeps them as BigInt).
export const RIGHTS = Object.freeze({
  FD_DATASYNC: 1n << 0n, FD_READ: 1n << 1n, FD_SEEK: 1n << 2n, FD_FDSTAT_SET_FLAGS: 1n << 3n,
  FD_SYNC: 1n << 4n, FD_TELL: 1n << 5n, FD_WRITE: 1n << 6n, FD_ADVISE: 1n << 7n, FD_ALLOCATE: 1n << 8n,
  PATH_CREATE_DIRECTORY: 1n << 9n, PATH_CREATE_FILE: 1n << 10n, PATH_LINK_SOURCE: 1n << 11n,
  PATH_LINK_TARGET: 1n << 12n, PATH_OPEN: 1n << 13n, FD_READDIR: 1n << 14n, PATH_READLINK: 1n << 15n,
  PATH_RENAME_SOURCE: 1n << 16n, PATH_RENAME_TARGET: 1n << 17n, PATH_FILESTAT_GET: 1n << 18n,
  PATH_FILESTAT_SET_SIZE: 1n << 19n, PATH_FILESTAT_SET_TIMES: 1n << 20n, FD_FILESTAT_GET: 1n << 21n,
  FD_FILESTAT_SET_SIZE: 1n << 22n, FD_FILESTAT_SET_TIMES: 1n << 23n, PATH_SYMLINK: 1n << 24n,
  PATH_REMOVE_DIRECTORY: 1n << 25n, PATH_UNLINK_FILE: 1n << 26n, POLL_FD_READWRITE: 1n << 27n,
  SOCK_SHUTDOWN: 1n << 28n, SOCK_ACCEPT: 1n << 29n,
});
export const RIGHTS_ALL = (1n << 30n) - 1n;
export const RIGHTS_SOCKET =
  RIGHTS.FD_READ | RIGHTS.FD_WRITE | RIGHTS.FD_FDSTAT_SET_FLAGS | RIGHTS.FD_FILESTAT_GET |
  RIGHTS.POLL_FD_READWRITE | RIGHTS.SOCK_SHUTDOWN | RIGHTS.SOCK_ACCEPT;
export const RIGHTS_CHARDEV = RIGHTS.FD_READ | RIGHTS.FD_WRITE | RIGHTS.FD_FDSTAT_SET_FLAGS | RIGHTS.FD_FILESTAT_GET | RIGHTS.POLL_FD_READWRITE;

// Struct sizes and field offsets.
export const SUBSCRIPTION_SIZE = 48; // userdata u64 @0, tag u8 @8, clock{id u32 @16, timeout u64 @24, precision u64 @32, flags u16 @40} | fd u32 @16
export const EVENT_SIZE = 32; // userdata u64 @0, error u16 @8, type u8 @10, nbytes u64 @16, flags u16 @24
export const FILESTAT_SIZE = 64; // dev @0, ino @8, filetype u8 @16, nlink @24, size @32, atim @40, mtim @48, ctim @56
export const FDSTAT_SIZE = 24; // filetype u8 @0, flags u16 @2, rights_base u64 @8, rights_inheriting u64 @16
export const DIRENT_SIZE = 24; // d_next u64 @0, d_ino u64 @8, d_namlen u32 @16, d_type u8 @20

/** Every function `wasi_snapshot_preview1` defines. The shim implements each one. */
export const PREVIEW1_FUNCTIONS = Object.freeze([
  "args_get", "args_sizes_get", "environ_get", "environ_sizes_get", "clock_res_get", "clock_time_get",
  "fd_advise", "fd_allocate", "fd_close", "fd_datasync", "fd_fdstat_get", "fd_fdstat_set_flags",
  "fd_fdstat_set_rights", "fd_filestat_get", "fd_filestat_set_size", "fd_filestat_set_times", "fd_pread",
  "fd_prestat_get", "fd_prestat_dir_name", "fd_pwrite", "fd_read", "fd_readdir", "fd_renumber", "fd_seek",
  "fd_sync", "fd_tell", "fd_write", "path_create_directory", "path_filestat_get", "path_filestat_set_times",
  "path_link", "path_open", "path_readlink", "path_remove_directory", "path_rename", "path_symlink",
  "path_unlink_file", "poll_oneoff", "proc_exit", "proc_raise", "sched_yield", "random_get", "sock_accept",
  "sock_recv", "sock_send", "sock_shutdown",
]);
