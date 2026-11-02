//! The launcher: `sandbox --memory <bytes> --cpu <seconds> [--read <path>]...
//! [--write <folder>]... [--sockets <folder>] -- <program> [<argument>]...`.
//! It runs in a process of its own, after `exec`, so it restricts itself
//! with ordinary code rather than between `fork` and `exec` in the server.

use std::collections::BTreeMap;
use std::ffi::{CString, OsString};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use landlock::{
    ABI, Access, AccessFs, AccessNet, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset,
    RulesetAttr, RulesetCreatedAttr, RulesetStatus, Scope,
};
use seccompiler::{
    BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter,
    SeccompRule, TargetArch,
};

/// The newest Landlock version the sandbox asks for. Older kernels give
/// what they have.
const ABI_WANTED: ABI = ABI::V6;

struct Request {
    memory: u64,
    cpu: u64,
    read: Vec<PathBuf>,
    write: Vec<PathBuf>,
    /// Where the worker may make local sockets, to listen on.
    sockets: Option<PathBuf>,
    program: PathBuf,
    args: Vec<OsString>,
}

/// Runs the launcher with its arguments, the ones after `sandbox`. It only
/// returns if it couldn't start the program, with the exit code to use.
pub fn launch(args: impl IntoIterator<Item = OsString>) -> i32 {
    let request = match parse(args) {
        Ok(request) => request,
        Err(message) => {
            eprintln!("sandbox: {message}");
            return 2;
        }
    };
    if let Err(message) = restrict(&request) {
        eprintln!("sandbox: {message}");
        return 126;
    }
    let err = exec(&request);
    eprintln!("sandbox: couldn't run {}: {err}", request.program.display());
    127
}

fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Request, String> {
    let mut args = args.into_iter();
    let mut memory = None;
    let mut cpu = None;
    let mut read = Vec::new();
    let mut write = Vec::new();
    let mut sockets = None;
    let number = |value: Option<OsString>, name: &str| {
        value
            .and_then(|value| value.to_str()?.parse::<u64>().ok())
            .ok_or_else(|| format!("{name} needs a number"))
    };
    loop {
        let arg = args.next().ok_or("there's no -- before the program")?;
        match arg.to_str() {
            Some("--memory") => memory = Some(number(args.next(), "--memory")?),
            Some("--cpu") => cpu = Some(number(args.next(), "--cpu")?),
            Some("--read") => read.push(PathBuf::from(args.next().ok_or("--read needs a path")?)),
            Some("--write") => {
                write.push(PathBuf::from(args.next().ok_or("--write needs a folder")?));
            }
            Some("--sockets") => {
                sockets = Some(PathBuf::from(
                    args.next().ok_or("--sockets needs a folder")?,
                ));
            }
            Some("--") => break,
            _ => {
                return Err(format!(
                    "{} isn't something the sandbox knows",
                    arg.display()
                ));
            }
        }
    }
    let program = PathBuf::from(args.next().ok_or("there's no program after --")?);
    if !program.is_absolute() {
        return Err(format!("{} isn't a full path", program.display()));
    }
    Ok(Request {
        memory: memory.ok_or("--memory is missing")?,
        cpu: cpu.ok_or("--cpu is missing")?,
        read,
        write,
        sockets,
        program,
        args: args.collect(),
    })
}

fn restrict(request: &Request) -> Result<(), String> {
    lower_priority();
    limit(request)?;
    // The program's interpreter, such as ld-linux, has to be allowed to run
    // too, because the kernel checks it like the program.
    let interpreter = interpreter(&request.program)
        .map_err(|err| format!("couldn't read {}: {err}", request.program.display()))?;
    landlock(request, interpreter.as_deref())?;
    seccomp(request.sockets.is_some())
}

/// Runs the worker at low priority for the CPU and the disk, so the
/// server's requests come first.
fn lower_priority() {
    // Failing to lower its own priority doesn't make the worker less safe,
    // so it carries on.
    unsafe {
        libc::setpriority(libc::PRIO_PROCESS, 0, 10);
        // The idle class: the worker reads the disk only when nothing else
        // is waiting for it.
        const IOPRIO_CLASS_IDLE: libc::c_long = 3;
        const IOPRIO_WHO_PROCESS: libc::c_long = 1;
        libc::syscall(
            libc::SYS_ioprio_set,
            IOPRIO_WHO_PROCESS,
            0,
            IOPRIO_CLASS_IDLE << 13,
        );
    }
}

fn limit(request: &Request) -> Result<(), String> {
    let set = |resource, soft: u64, hard: u64, name: &str| {
        let limit = libc::rlimit {
            rlim_cur: soft,
            rlim_max: hard,
        };
        // Lowering a hard limit is permanent for a process without
        // privileges, so the worker can't raise it again.
        if unsafe { libc::setrlimit(resource, &limit) } == 0 {
            Ok(())
        } else {
            Err(format!(
                "couldn't limit its {name}: {}",
                io::Error::last_os_error()
            ))
        }
    };
    // Writable memory, rather than address space: decoders reserve far more
    // address space than they use. libheif reserves about 1.5 GB to decode a
    // 7-megapixel HEIC photo in 120 MB, for example.
    set(libc::RLIMIT_DATA, request.memory, request.memory, "memory")?;
    // At the soft limit the kernel sends SIGXCPU, and a second later SIGKILL.
    set(libc::RLIMIT_CPU, request.cpu, request.cpu + 1, "CPU time")?;
    // A core dump would hold part of someone's file.
    set(libc::RLIMIT_CORE, 0, 0, "core dumps")?;
    set(libc::RLIMIT_NOFILE, 256, 256, "open files")?;
    Ok(())
}

fn landlock(request: &Request, interpreter: Option<&Path>) -> Result<(), String> {
    let fail = |err: &dyn std::fmt::Display| format!("couldn't set up Landlock: {err}");
    let read = AccessFs::from_read(ABI_WANTED) & !AccessFs::Execute;
    let mut ruleset = Ruleset::default()
        // Landlock itself is required. Newer parts of it, such as network
        // and signal rules, are used where the kernel has them.
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(AccessFs::from_all(ABI_WANTED))
        .map_err(|err| fail(&err))?
        .handle_access(AccessNet::from_all(ABI_WANTED))
        .map_err(|err| fail(&err))?
        .scope(Scope::from_all(ABI_WANTED))
        .map_err(|err| fail(&err))?
        .create()
        .map_err(|err| fail(&err))?;
    for path in &request.read {
        // A file, such as the one a worker reads by name, can only be read.
        // Listing applies to folders.
        let access = if path.is_dir() {
            read
        } else {
            AccessFs::ReadFile.into()
        };
        let fd = PathFd::new(path).map_err(|err| fail(&err))?;
        ruleset = ruleset
            .add_rule(PathBeneath::new(fd, access))
            .map_err(|err| fail(&err))?;
    }
    // A scratch folder: its files can be read, written, renamed, and
    // removed, but not run, so a worker can't write a program and start it.
    // Nor can it make links, which could point the server at another file
    // when it reads what the worker wrote.
    let write = AccessFs::from_all(ABI_WANTED)
        & !(AccessFs::Execute
            | AccessFs::MakeSym
            | AccessFs::MakeChar
            | AccessFs::MakeBlock
            | AccessFs::MakeFifo
            | AccessFs::MakeSock
            | AccessFs::IoctlDev);
    for folder in &request.write {
        let fd = PathFd::new(folder).map_err(|err| fail(&err))?;
        ruleset = ruleset
            .add_rule(PathBeneath::new(fd, write))
            .map_err(|err| fail(&err))?;
    }
    // Only new sockets: the folder's files stay out of reach.
    if let Some(folder) = &request.sockets {
        let fd = PathFd::new(folder).map_err(|err| fail(&err))?;
        ruleset = ruleset
            .add_rule(PathBeneath::new(fd, AccessFs::MakeSock))
            .map_err(|err| fail(&err))?;
    }
    for program in std::iter::once(request.program.as_path()).chain(interpreter) {
        let fd = PathFd::new(program).map_err(|err| fail(&err))?;
        ruleset = ruleset
            .add_rule(PathBeneath::new(fd, AccessFs::Execute | AccessFs::ReadFile))
            .map_err(|err| fail(&err))?;
    }
    let status = ruleset.restrict_self().map_err(|err| fail(&err))?;
    if status.ruleset == RulesetStatus::NotEnforced {
        return Err(crate::Unavailable::NoLandlock.to_string());
    }
    Ok(())
}

/// The system calls the worker has no use for, and a filter that refuses
/// them. With `sockets`, the worker may listen on a local socket of its
/// own, which LibreOffice needs, but still connect to nothing.
fn seccomp(sockets: bool) -> Result<(), String> {
    let fail = |err: &dyn std::fmt::Display| format!("couldn't set up seccomp: {err}");
    let arch = TargetArch::try_from(std::env::consts::ARCH).map_err(|err| fail(&err))?;
    let pid = std::process::id();

    let mut refused: BTreeMap<i64, Vec<SeccompRule>> = [
        // Networks and other processes' sockets. Socket pairs stay, since
        // they only connect a process to itself.
        libc::SYS_connect,
        // Other processes' memory.
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_pidfd_open,
        libc::SYS_pidfd_getfd,
        libc::SYS_pidfd_send_signal,
        // io_uring runs system calls that seccomp never sees.
        libc::SYS_io_uring_setup,
        libc::SYS_io_uring_enter,
        libc::SYS_io_uring_register,
        // Namespaces and the kernel's more exposed corners.
        libc::SYS_unshare,
        libc::SYS_setns,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_pivot_root,
        libc::SYS_chroot,
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
        libc::SYS_userfaultfd,
        libc::SYS_keyctl,
        libc::SYS_add_key,
        libc::SYS_request_key,
        libc::SYS_personality,
        // A signal to one thread, named only by its ID, which could be
        // another process's. Programs use tgkill instead.
        libc::SYS_tkill,
    ]
    .into_iter()
    .map(|call| (call, Vec::new()))
    .collect();
    if !sockets {
        for call in [
            libc::SYS_socket,
            libc::SYS_bind,
            libc::SYS_listen,
            libc::SYS_accept,
            libc::SYS_accept4,
        ] {
            refused.insert(call, Vec::new());
        }
    }

    let rule =
        |conditions: Vec<SeccompCondition>| SeccompRule::new(conditions).map_err(|err| fail(&err));
    let arg = |index, op, value| {
        SeccompCondition::new(index, SeccompCmpArgLen::Dword, op, value).map_err(|err| fail(&err))
    };

    if sockets {
        // Only local stream sockets, which can send nothing until they
        // connect. A datagram socket could send to another program's
        // socket without connecting. The type's low bits are the kind of
        // socket, and the rest are flags.
        let mut socket_rules = vec![rule(vec![arg(0, SeccompCmpOp::Ne, libc::AF_UNIX as u64)?])?];
        for kind in (0..16).filter(|&kind| kind != libc::SOCK_STREAM as u64) {
            socket_rules.push(rule(vec![arg(1, SeccompCmpOp::MaskedEq(0xf), kind)?])?);
        }
        refused.insert(libc::SYS_socket, socket_rules);
    }

    // Signals only to the worker itself and its process group, whose ID is
    // its own, since the server starts it in a new group.
    let own = u64::from(pid);
    let group = u64::from(pid.wrapping_neg());
    refused.insert(
        libc::SYS_kill,
        vec![rule(vec![
            arg(0, SeccompCmpOp::Ne, 0)?,
            arg(0, SeccompCmpOp::Ne, own)?,
            arg(0, SeccompCmpOp::Ne, group)?,
        ])?],
    );
    for call in [
        libc::SYS_tgkill,
        libc::SYS_rt_sigqueueinfo,
        libc::SYS_rt_tgsigqueueinfo,
    ] {
        refused.insert(call, vec![rule(vec![arg(0, SeccompCmpOp::Ne, own)?])?]);
    }

    // New threads and processes, but not new namespaces. Each rule matches
    // one namespace flag.
    let namespaces = [
        libc::CLONE_NEWNS,
        libc::CLONE_NEWCGROUP,
        libc::CLONE_NEWUTS,
        libc::CLONE_NEWIPC,
        libc::CLONE_NEWUSER,
        libc::CLONE_NEWPID,
        libc::CLONE_NEWNET,
    ];
    let mut clone_rules = Vec::new();
    for flag in namespaces {
        let flag = flag as u64;
        clone_rules.push(rule(vec![arg(0, SeccompCmpOp::MaskedEq(flag), flag)?])?);
    }
    refused.insert(libc::SYS_clone, clone_rules);

    let refuse = SeccompFilter::new(
        refused,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EPERM as u32),
        arch,
    )
    .map_err(|err| fail(&err))?;
    // clone3 passes its flags in memory, where seccomp can't read them. It
    // answers "not implemented", so the C library falls back to clone, which
    // the filter above checks.
    let clone3 = SeccompFilter::new(
        BTreeMap::from([(libc::SYS_clone3, Vec::new())]),
        SeccompAction::Allow,
        SeccompAction::Errno(libc::ENOSYS as u32),
        arch,
    )
    .map_err(|err| fail(&err))?;
    for filter in [refuse, clone3] {
        let program = BpfProgram::try_from(filter).map_err(|err| fail(&err))?;
        seccompiler::apply_filter(&program).map_err(|err| fail(&err))?;
    }
    Ok(())
}

/// The interpreter a program names, such as `/lib64/ld-linux-x86-64.so.2`,
/// or `None` if it has none. Only 64-bit little-endian ELF programs run,
/// the kind on every system Accordion supports.
fn interpreter(program: &Path) -> io::Result<Option<PathBuf>> {
    const PT_INTERP: u32 = 3;
    let unsupported = || io::Error::new(io::ErrorKind::InvalidData, "it isn't a 64-bit program");
    let mut file = File::open(program)?;
    let mut header = [0; 64];
    file.read_exact(&mut header)?;
    if &header[..4] != b"\x7fELF" || header[4] != 2 || header[5] != 1 {
        return Err(unsupported());
    }
    let u16_at = |at: usize| u16::from_le_bytes([header[at], header[at + 1]]);
    let phoff = u64::from_le_bytes(header[0x20..0x28].try_into().unwrap());
    let phentsize = u64::from(u16_at(0x36));
    let phnum = u64::from(u16_at(0x38));
    for index in 0..phnum {
        let mut entry = [0; 56];
        file.seek(SeekFrom::Start(phoff + index * phentsize))?;
        file.read_exact(&mut entry)?;
        if u32::from_le_bytes(entry[..4].try_into().unwrap()) != PT_INTERP {
            continue;
        }
        let offset = u64::from_le_bytes(entry[8..16].try_into().unwrap());
        let size = u64::from_le_bytes(entry[32..40].try_into().unwrap());
        if size > 4096 {
            return Err(unsupported());
        }
        let mut path = vec![0; size as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut path)?;
        let path = path.split(|&byte| byte == 0).next().unwrap_or_default();
        return Ok(Some(PathBuf::from(std::ffi::OsStr::from_bytes(path))));
    }
    Ok(None)
}

fn exec(request: &Request) -> io::Error {
    let cstring = |bytes: &[u8]| CString::new(bytes).map_err(io::Error::other);
    let program = match cstring(request.program.as_os_str().as_bytes()) {
        Ok(program) => program,
        Err(err) => return err,
    };
    let mut argv = vec![program.clone()];
    for arg in &request.args {
        match cstring(arg.as_bytes()) {
            Ok(arg) => argv.push(arg),
            Err(err) => return err,
        }
    }
    let mut pointers: Vec<*const libc::c_char> = argv.iter().map(|arg| arg.as_ptr()).collect();
    pointers.push(std::ptr::null());
    // The environment is the one the server gave the launcher.
    unsafe { libc::execv(program.as_ptr(), pointers.as_ptr()) };
    io::Error::last_os_error()
}
