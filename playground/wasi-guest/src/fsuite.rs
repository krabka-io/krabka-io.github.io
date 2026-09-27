//! The file-system suite: the preview-1 file calls the broker's log makes,
//! each checked against its expected result.
//!
//! [`run`] starts from an empty directory, walks through directories, files,
//! positional I/O across the runtime's 64 KiB storage chunks, truncation and
//! sparse extension, renames (within and across directories, over an existing
//! file, of a directory), listing with removal during iteration, removal, file
//! times, and the raw calls std does not expose. It returns the number of
//! checks that passed, or the first failure.
//!
//! It leaves `keep/` behind: an append log, a 200 KB blob, a sparse file, a
//! file that replaced another by rename (the log's segment swap) and a nested
//! file. A test reads those back after a reload or a restart to prove the
//! volume persisted.

use std::fmt::Debug;
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

/// WASI preview-1 errno values the checks expect.
const EBADF: i32 = 8;
const EEXIST: i32 = 20;
const EISDIR: i32 = 31;
const ENOENT: i32 = 44;
const ENOTDIR: i32 = 54;
const ENOTEMPTY: i32 = 55;
const ENOSYS: i32 = 52;
const ENOTSUP: i32 = 58;

/// Runs the suite in `base`, which it recreates first.
pub fn run(base: &Path) -> Result<usize, String> {
    let mut suite = Suite::default();
    suite.all(base)?;
    Ok(suite.checks)
}

/// A deterministic byte pattern, different for every seed.
pub fn pattern(len: usize, seed: u32) -> Vec<u8> {
    let mut state = seed.wrapping_mul(2_654_435_761).wrapping_add(1);
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            state.to_be_bytes()[0]
        })
        .collect()
}

/// Names the step an I/O error came from.
fn step(name: &'static str) -> impl FnOnce(io::Error) -> String {
    move |err| format!("{name}: {err}")
}

/// Names the step a raw WASI call failed in.
fn call(name: &'static str) -> impl FnOnce(wasi::Errno) -> String {
    move |err| format!("{name}: errno {}", err.raw())
}

/// Names the step a rustix call failed in.
fn sys(name: &'static str) -> impl FnOnce(rustix::io::Errno) -> String {
    move |err| format!("{name}: {err}")
}

fn raw(fd: RawFd) -> Result<u32, String> {
    u32::try_from(fd).map_err(|err| format!("descriptor {fd}: {err}"))
}

/// The cursor of `file`, through `fd_tell`.
fn tell(file: &File) -> Result<u64, String> {
    let fd = raw(file.as_raw_fd())?;
    // SAFETY: `fd` belongs to `file`, which outlives the call.
    unsafe { wasi::fd_tell(fd) }.map_err(call("fd_tell"))
}

#[derive(Default)]
struct Suite {
    checks: usize,
}

impl Suite {
    fn check(
        &mut self,
        name: &str,
        ok: bool,
        detail: impl FnOnce() -> String,
    ) -> Result<(), String> {
        if ok {
            self.checks += 1;
            Ok(())
        } else {
            Err(format!("{name}: {}", detail()))
        }
    }

    fn eq<T: PartialEq + Debug>(&mut self, name: &str, got: T, want: T) -> Result<(), String> {
        let ok = got == want;
        self.check(name, ok, move || format!("got {got:?}, want {want:?}"))
    }

    /// Checks that `result` failed with the WASI errno `want`.
    fn errno<T: Debug>(
        &mut self,
        name: &str,
        result: io::Result<T>,
        want: i32,
    ) -> Result<(), String> {
        match result {
            Ok(value) => Err(format!(
                "{name}: succeeded with {value:?}, want errno {want}"
            )),
            Err(err) => self.eq(name, err.raw_os_error(), Some(want)),
        }
    }

    /// Checks that a raw WASI call failed with the errno `want`.
    fn wasi_errno<T: Debug>(
        &mut self,
        name: &str,
        result: Result<T, wasi::Errno>,
        want: i32,
    ) -> Result<(), String> {
        match result {
            Ok(value) => Err(format!(
                "{name}: succeeded with {value:?}, want errno {want}"
            )),
            Err(err) => self.eq(name, i32::from(err.raw()), want),
        }
    }

    fn all(&mut self, base: &Path) -> Result<(), String> {
        if fs::symlink_metadata(base).is_ok() {
            fs::remove_dir_all(base).map_err(step("clear the previous run"))?;
        }
        self.directories(base)?;
        self.files(base)?;
        self.chunks(base)?;
        self.sizes(base)?;
        self.renames(base)?;
        self.listing(base)?;
        self.removal(base)?;
        self.times(base)?;
        self.raw_calls(base)?;
        self.keep(&base.join("keep"))
    }

    fn directories(&mut self, base: &Path) -> Result<(), String> {
        fs::create_dir_all(base.join("sub")).map_err(step("create_dir_all"))?;
        let is_dir = base.join("sub").is_dir();
        self.check("mkdir", is_dir, || "sub is not a directory".into())?;
        self.errno(
            "mkdir over a directory",
            fs::create_dir(base.join("sub")),
            EEXIST,
        )?;
        self.errno(
            "mkdir under a missing parent",
            fs::create_dir(base.join("missing/child")),
            ENOENT,
        )?;
        let meta = fs::metadata(base).map_err(step("stat a directory"))?;
        self.check("stat a directory", meta.is_dir(), || format!("{meta:?}"))
    }

    fn files(&mut self, base: &Path) -> Result<(), String> {
        let a = base.join("a.txt");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&a)
            .map_err(step("O_CREAT|O_EXCL"))?;
        self.errno(
            "O_EXCL on an existing file",
            OpenOptions::new().write(true).create_new(true).open(&a),
            EEXIST,
        )?;
        file.write_all(b"hello").map_err(step("write"))?;
        self.eq(
            "size after a write",
            file.metadata().map_err(step("fstat"))?.len(),
            5,
        )?;
        drop(file);

        let mut file = OpenOptions::new()
            .append(true)
            .open(&a)
            .map_err(step("open with O_APPEND"))?;
        file.write_all(b" world").map_err(step("append"))?;
        file.seek(SeekFrom::Start(0))
            .map_err(step("seek an append descriptor"))?;
        file.write_all(b"!").map_err(step("append after a seek"))?;
        drop(file);
        self.eq(
            "O_APPEND writes at the end",
            fs::read(&a).map_err(step("read back"))?,
            b"hello world!".to_vec(),
        )?;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&a)
            .map_err(step("open read-write"))?;
        let written = rustix::io::pwrite(&file, b"WORLD", 6).map_err(sys("pwrite"))?;
        self.eq("pwrite", written, 5)?;
        self.eq("pwrite leaves the cursor alone", tell(&file)?, 0)?;
        let mut word = [0u8; 5];
        let read = rustix::io::pread(&file, &mut word, 6).map_err(sys("pread"))?;
        self.eq("pread", (read, &word), (5, b"WORLD"))?;
        self.eq(
            "seek from the end",
            file.seek(SeekFrom::End(-6)).map_err(step("seek"))?,
            6,
        )?;
        file.read_exact(&mut word)
            .map_err(step("read after a seek"))?;
        self.eq("read after a seek", &word, b"WORLD")?;
        self.eq("fd_tell", tell(&file)?, 11)?;
        self.eq(
            "stream_position",
            file.stream_position().map_err(step("stream_position"))?,
            11,
        )?;
        drop(file);

        let mut write_only = OpenOptions::new()
            .write(true)
            .open(&a)
            .map_err(step("open write-only"))?;
        self.errno(
            "read from a write-only descriptor",
            write_only.read(&mut word),
            EBADF,
        )
    }

    fn chunks(&mut self, base: &Path) -> Result<(), String> {
        let path = base.join("big.bin");
        let mut expected = pattern(200_000, 1);
        fs::write(&path, &expected).map_err(step("write 200 000 bytes"))?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(step("open big.bin"))?;
        let overlay = pattern(10_000, 2);
        rustix::io::pwrite(&file, &overlay, 65_530)
            .map_err(sys("pwrite across a chunk boundary"))?;
        expected[65_530..75_530].copy_from_slice(&overlay);
        let mut window = vec![0u8; 131_072];
        let read = rustix::io::pread(&file, &mut window[..], 60_000)
            .map_err(sys("pread across chunks"))?;
        self.eq("pread across two chunk boundaries", read, 131_072)?;
        let same = window[..] == expected[60_000..191_072];
        self.check("bytes across chunk boundaries", same, || {
            "the window differs".into()
        })?;
        let whole = fs::read(&path).map_err(step("read big.bin"))?;
        let same = whole == expected;
        self.check("the whole 200 000 bytes", same, || {
            format!("{} bytes read", whole.len())
        })
    }

    fn sizes(&mut self, base: &Path) -> Result<(), String> {
        let a = base.join("a.txt");
        let file = OpenOptions::new()
            .write(true)
            .open(&a)
            .map_err(step("open a.txt"))?;
        file.set_len(3).map_err(step("truncate"))?;
        self.eq(
            "truncate",
            fs::read(&a).map_err(step("read"))?,
            b"hel".to_vec(),
        )?;
        file.set_len(10).map_err(step("extend"))?;
        self.eq(
            "extend fills with zeros",
            fs::read(&a).map_err(step("read"))?,
            b"hel\0\0\0\0\0\0\0".to_vec(),
        )?;
        drop(file);

        let mut file = File::create(&a).map_err(step("O_TRUNC"))?;
        self.eq(
            "O_TRUNC empties the file",
            file.metadata().map_err(step("fstat"))?.len(),
            0,
        )?;
        file.write_all(b"fresh").map_err(step("write"))?;
        file.sync_all().map_err(step("fsync"))?;
        file.sync_data().map_err(step("fdatasync"))?;
        drop(file);

        let sparse = base.join("sparse.bin");
        let file = File::create(&sparse).map_err(step("create sparse.bin"))?;
        file.set_len(1 << 20).map_err(step("extend to 1 MiB"))?;
        rustix::io::pwrite(&file, &[0xAB], 900_000).map_err(sys("pwrite into a hole"))?;
        let bytes = fs::read(&sparse).map_err(step("read sparse.bin"))?;
        self.eq("sparse size", bytes.len(), 1 << 20)?;
        let holes = bytes
            .iter()
            .enumerate()
            .all(|(i, &b)| b == if i == 900_000 { 0xAB } else { 0 });
        self.check("holes read as zeros", holes, || "a hole holds data".into())?;

        let file = File::create(base.join("alloc.bin")).map_err(step("create alloc.bin"))?;
        let fd = raw(file.as_raw_fd())?;
        // SAFETY: `fd` belongs to `file`, which outlives both calls.
        unsafe { wasi::fd_advise(fd, 0, 4096, wasi::ADVICE_SEQUENTIAL) }
            .map_err(call("fd_advise"))?;
        // SAFETY: as above.
        unsafe { wasi::fd_allocate(fd, 0, 4096) }.map_err(call("fd_allocate"))?;
        self.eq(
            "fd_allocate extends",
            file.metadata().map_err(step("fstat"))?.len(),
            4096,
        )
    }

    fn renames(&mut self, base: &Path) -> Result<(), String> {
        fs::rename(base.join("a.txt"), base.join("b.txt")).map_err(step("rename"))?;
        self.errno(
            "the old name is gone",
            fs::metadata(base.join("a.txt")),
            ENOENT,
        )?;
        self.eq(
            "rename keeps the bytes",
            fs::read(base.join("b.txt")).map_err(step("read"))?,
            b"fresh".to_vec(),
        )?;

        fs::rename(base.join("b.txt"), base.join("sub/c.txt"))
            .map_err(step("rename across directories"))?;
        self.eq(
            "rename across directories",
            fs::read(base.join("sub/c.txt")).map_err(step("read"))?,
            b"fresh".to_vec(),
        )?;

        fs::write(base.join("sub/d.txt"), b"D").map_err(step("write d.txt"))?;
        fs::rename(base.join("sub/c.txt"), base.join("sub/d.txt"))
            .map_err(step("rename over a file"))?;
        self.eq(
            "rename over an existing file",
            fs::read(base.join("sub/d.txt")).map_err(step("read"))?,
            b"fresh".to_vec(),
        )?;
        self.errno(
            "the replacing file's old name is gone",
            fs::metadata(base.join("sub/c.txt")),
            ENOENT,
        )?;

        fs::rename(base.join("sub"), base.join("sub2")).map_err(step("rename a directory"))?;
        self.eq(
            "a renamed directory carries its files",
            fs::read(base.join("sub2/d.txt")).map_err(step("read"))?,
            b"fresh".to_vec(),
        )?;
        self.errno(
            "the old directory name is gone",
            fs::metadata(base.join("sub")),
            ENOENT,
        )?;

        fs::create_dir(base.join("sub2/inner")).map_err(step("mkdir inner"))?;
        let into_itself = fs::rename(base.join("sub2"), base.join("sub2/inner/x"));
        self.check(
            "a directory cannot move into itself",
            into_itself.is_err(),
            || "it did".into(),
        )?;
        self.errno(
            "rename a file over a directory",
            fs::rename(base.join("sub2/d.txt"), base.join("sub2/inner")),
            EISDIR,
        )?;
        self.errno(
            "rename a directory over a file",
            fs::rename(base.join("sub2/inner"), base.join("sub2/d.txt")),
            ENOTDIR,
        )
    }

    fn listing(&mut self, base: &Path) -> Result<(), String> {
        let many = base.join("many");
        fs::create_dir(&many).map_err(step("mkdir many"))?;
        for i in 0..300 {
            fs::write(many.join(format!("f{i:03}")), i.to_string())
                .map_err(step("write an entry"))?;
        }
        let mut names = fs::read_dir(&many)
            .map_err(step("read_dir"))?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<io::Result<Vec<_>>>()
            .map_err(step("read_dir entry"))?;
        names.sort();
        let want: Vec<String> = (0..300).map(|i| format!("f{i:03}")).collect();
        self.eq("read_dir lists every entry once", names, want)?;

        let mut removed = 0;
        for entry in fs::read_dir(&many).map_err(step("read_dir"))? {
            fs::remove_file(entry.map_err(step("read_dir entry"))?.path())
                .map_err(step("remove while listing"))?;
            removed += 1;
        }
        self.eq(
            "removal during a listing keeps the cookies valid",
            removed,
            300,
        )?;
        self.eq(
            "an emptied directory",
            fs::read_dir(&many).map_err(step("read_dir"))?.count(),
            0,
        )?;
        fs::remove_dir(&many).map_err(step("rmdir many"))
    }

    fn removal(&mut self, base: &Path) -> Result<(), String> {
        let sub2 = base.join("sub2");
        self.errno(
            "rmdir a non-empty directory",
            fs::remove_dir(&sub2),
            ENOTEMPTY,
        )?;
        self.errno(
            "unlink a directory",
            fs::remove_file(sub2.join("inner")),
            EISDIR,
        )?;
        self.errno("rmdir a file", fs::remove_dir(sub2.join("d.txt")), ENOTDIR)?;
        fs::remove_file(sub2.join("d.txt")).map_err(step("unlink"))?;
        self.errno(
            "an unlinked file is gone",
            fs::metadata(sub2.join("d.txt")),
            ENOENT,
        )?;
        fs::remove_dir(sub2.join("inner")).map_err(step("rmdir inner"))?;
        fs::remove_dir(&sub2).map_err(step("rmdir sub2"))?;
        self.errno("a removed directory is gone", fs::metadata(&sub2), ENOENT)?;

        let ghost = base.join("ghost.bin");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&ghost)
            .map_err(step("create ghost.bin"))?;
        file.write_all(b"boo").map_err(step("write ghost.bin"))?;
        fs::remove_file(&ghost).map_err(step("unlink an open file"))?;
        self.errno(
            "an unlinked open file has no name",
            fs::metadata(&ghost),
            ENOENT,
        )?;
        file.write_all(b"!!")
            .map_err(step("write an unlinked file"))?;
        let mut word = [0u8; 5];
        rustix::io::pread(&file, &mut word, 0).map_err(sys("pread an unlinked file"))?;
        self.eq("an unlinked open file keeps its bytes", &word, b"boo!!")?;
        self.eq(
            "fstat an unlinked file",
            file.metadata().map_err(step("fstat"))?.len(),
            5,
        )
    }

    fn times(&mut self, base: &Path) -> Result<(), String> {
        let big = base.join("big.bin");
        let meta = fs::symlink_metadata(&big).map_err(step("lstat"))?;
        self.check(
            "stat a file",
            meta.is_file() && meta.len() == 200_000,
            || format!("{meta:?}"),
        )?;

        let modified = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        let accessed = modified + Duration::from_secs(1);
        let file = OpenOptions::new()
            .write(true)
            .open(&big)
            .map_err(step("open big.bin"))?;
        file.set_times(
            FileTimes::new()
                .set_modified(modified)
                .set_accessed(accessed),
        )
        .map_err(step("fd_filestat_set_times"))?;
        let meta = fs::metadata(&big).map_err(step("stat"))?;
        self.eq(
            "mtime to the nanosecond",
            meta.modified().map_err(step("mtime"))?,
            modified,
        )?;
        self.eq(
            "atime to the nanosecond",
            meta.accessed().map_err(step("atime"))?,
            accessed,
        )?;

        let dir = File::open(base).map_err(step("open the base directory"))?;
        let dirfd = raw(dir.as_raw_fd())?;
        let stamp: u64 = 1_600_000_000_000_000_000;
        // SAFETY: `dirfd` belongs to `dir`, which outlives the call.
        unsafe {
            wasi::path_filestat_set_times(
                dirfd,
                0,
                "big.bin",
                stamp,
                stamp,
                wasi::FSTFLAGS_ATIM | wasi::FSTFLAGS_MTIM,
            )
        }
        .map_err(call("path_filestat_set_times"))?;
        let meta = fs::metadata(&big).map_err(step("stat"))?;
        self.eq(
            "path_filestat_set_times",
            meta.modified().map_err(step("mtime"))?,
            UNIX_EPOCH + Duration::from_nanos(stamp),
        )
    }

    fn raw_calls(&mut self, base: &Path) -> Result<(), String> {
        let dir = File::open(base).map_err(step("open the base directory"))?;
        let dirfd = raw(dir.as_raw_fd())?;
        let rights = wasi::RIGHTS_FD_READ | wasi::RIGHTS_FD_READDIR;
        // SAFETY: `dirfd` belongs to `dir`, which outlives every call below;
        // the descriptors `path_open` returns are closed before it ends.
        let on_file = unsafe {
            wasi::path_open(
                dirfd,
                0,
                "big.bin",
                wasi::OFLAGS_DIRECTORY,
                rights,
                rights,
                0,
            )
        };
        self.wasi_errno("O_DIRECTORY on a file", on_file, ENOTDIR)?;
        fs::create_dir(base.join("d")).map_err(step("mkdir d"))?;
        // SAFETY: as above.
        let fd =
            unsafe { wasi::path_open(dirfd, 0, "d", wasi::OFLAGS_DIRECTORY, rights, rights, 0) }
                .map_err(call("O_DIRECTORY on a directory"))?;
        // SAFETY: `fd` was just opened and is closed right after.
        let stat = unsafe { wasi::fd_fdstat_get(fd) }.map_err(call("fd_fdstat_get"))?;
        self.eq(
            "O_DIRECTORY opens a directory",
            stat.fs_filetype,
            wasi::FILETYPE_DIRECTORY,
        )?;
        // SAFETY: as above.
        unsafe { wasi::fd_close(fd) }.map_err(call("fd_close"))?;

        self.errno(
            "hard links are not supported",
            fs::hard_link(base.join("big.bin"), base.join("big.link")),
            ENOTSUP,
        )?;
        // SAFETY: as above.
        let symlink = unsafe { wasi::path_symlink("big.bin", dirfd, "big.sym") };
        self.wasi_errno("symlinks are not supported", symlink, ENOTSUP)?;
        self.errno(
            "readlink is not supported",
            fs::read_link(base.join("big.bin")),
            ENOTSUP,
        )?;

        // Dropping FD_WRITE from a descriptor's rights makes its writes fail.
        let mut narrowed = OpenOptions::new()
            .read(true)
            .write(true)
            .open(base.join("big.bin"))
            .map_err(step("open big.bin"))?;
        let fd = raw(narrowed.as_raw_fd())?;
        // SAFETY: `fd` belongs to `narrowed`, which outlives the calls.
        let stat = unsafe { wasi::fd_fdstat_get(fd) }.map_err(call("fd_fdstat_get"))?;
        // SAFETY: as above; the new rights are a subset of the old ones.
        unsafe {
            wasi::fd_fdstat_set_rights(
                fd,
                stat.fs_rights_base & !wasi::RIGHTS_FD_WRITE,
                stat.fs_rights_inheriting,
            )
        }
        .map_err(call("fd_fdstat_set_rights"))?;
        self.errno("a write without FD_WRITE", narrowed.write(b"x"), EBADF)?;
        // SAFETY: `proc_raise` takes a signal number and nothing else.
        let raised = unsafe { wasi::proc_raise(wasi::SIGNAL_USR1) };
        self.wasi_errno("proc_raise answers ENOSYS", raised, ENOSYS)?;

        fs::write(base.join("x"), b"xx").map_err(step("write x"))?;
        fs::write(base.join("y"), b"yy").map_err(step("write y"))?;
        let x = File::open(base.join("x"))
            .map_err(step("open x"))?
            .into_raw_fd();
        let y = File::open(base.join("y"))
            .map_err(step("open y"))?
            .into_raw_fd();
        // SAFETY: both descriptors were just opened and nothing else owns them;
        // after the call `y` names what `x` named and `x` is closed.
        unsafe { wasi::fd_renumber(raw(x)?, raw(y)?) }.map_err(call("fd_renumber"))?;
        // SAFETY: `y` is open and owned by nobody else.
        let mut moved = unsafe { File::from_raw_fd(y) };
        let mut text = String::new();
        moved
            .read_to_string(&mut text)
            .map_err(step("read the renumbered descriptor"))?;
        self.eq("fd_renumber moves the description", text.as_str(), "xx")?;
        // SAFETY: `fd_fdstat_get` only reads; `x` is expected to be closed.
        let closed = unsafe { wasi::fd_fdstat_get(raw(x)?) }.map(|stat| stat.fs_filetype);
        self.wasi_errno("fd_renumber closes the source", closed, EBADF)?;

        // std's `sync_data` is `fsync` on wasi; the log's durability barrier
        // may still arrive as `fd_datasync` from other code.
        let file = OpenOptions::new()
            .write(true)
            .open(base.join("big.bin"))
            .map_err(step("open big.bin"))?;
        // SAFETY: the descriptor belongs to `file`, which outlives the call.
        let synced = unsafe { wasi::fd_datasync(raw(file.as_raw_fd())?) };
        self.check("fd_datasync", synced.is_ok(), || format!("{synced:?}"))?;

        std::thread::yield_now();
        // SAFETY: `sched_yield` has no arguments.
        unsafe { wasi::sched_yield() }.map_err(call("sched_yield"))?;
        let mut first = [0u8; 32];
        let mut second = [0u8; 32];
        // SAFETY: each buffer is 32 writable bytes.
        unsafe { wasi::random_get(first.as_mut_ptr(), first.len()) }.map_err(call("random_get"))?;
        // SAFETY: as above.
        unsafe { wasi::random_get(second.as_mut_ptr(), second.len()) }
            .map_err(call("random_get"))?;
        self.check("random_get", first != [0; 32] && first != second, || {
            format!("{first:?} {second:?}")
        })?;
        // SAFETY: the clock calls only return values.
        let resolution = unsafe { wasi::clock_res_get(wasi::CLOCKID_MONOTONIC) }
            .map_err(call("clock_res_get"))?;
        self.check("clock_res_get", resolution > 0, || format!("{resolution}"))?;
        // SAFETY: as above.
        let realtime = unsafe { wasi::clock_time_get(wasi::CLOCKID_REALTIME, 1) }
            .map_err(call("clock_time_get"))?;
        self.check(
            "a plausible realtime clock",
            realtime > 1_500_000_000_000_000_000,
            || format!("{realtime}"),
        )
    }

    /// Leaves the files a persistence test reads back after a reload.
    fn keep(&mut self, dir: &Path) -> Result<(), String> {
        fs::create_dir_all(dir.join("nested/deep")).map_err(step("mkdir keep"))?;
        fs::write(dir.join("blob.bin"), pattern(200_000, 7)).map_err(step("write blob.bin"))?;
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("log.txt"))
            .map_err(step("open log.txt"))?;
        for i in 0..100 {
            writeln!(log, "line {i:03} of the append log").map_err(step("append a line"))?;
            if i % 10 == 9 {
                log.sync_data().map_err(step("fdatasync the log"))?;
            }
        }
        let sparse =
            File::create(dir.join("sparse.bin")).map_err(step("create keep/sparse.bin"))?;
        sparse
            .set_len(300_000)
            .map_err(step("extend keep/sparse.bin"))?;
        rustix::io::pwrite(&sparse, b"tail", 250_000).map_err(sys("pwrite keep/sparse.bin"))?;
        fs::write(dir.join("renamed.dat"), b"old contents").map_err(step("write renamed.dat"))?;
        fs::write(dir.join("segment.swap"), pattern(70_000, 9))
            .map_err(step("write segment.swap"))?;
        fs::rename(dir.join("segment.swap"), dir.join("renamed.dat"))
            .map_err(step("swap a segment"))?;
        fs::write(dir.join("nested/deep/file.txt"), "nested \u{2713}\n")
            .map_err(step("write a nested file"))?;
        let swapped = fs::read(dir.join("renamed.dat")).map_err(step("read renamed.dat"))?;
        let same = swapped == pattern(70_000, 9);
        self.check("a swapped segment", same, || {
            format!("{} bytes", swapped.len())
        })
    }
}
