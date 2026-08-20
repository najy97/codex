//! Runtime-independent Unix supervision for local stdio MCP process groups.

use std::io;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;

pub const CODEX_MCP_PROCESS_SUPERVISOR_ARG1: &str = "--codex-mcp-process-supervisor";

const PARENT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const PROCESS_GROUP_TERM_GRACE_PERIOD: Duration = Duration::from_secs(2);
static MCP_PROCESS_SUPERVISOR_EXE: OnceLock<PathBuf> = OnceLock::new();

pub fn configure_mcp_process_supervisor_exe(path: PathBuf) {
    let _ = MCP_PROCESS_SUPERVISOR_EXE.set(path);
}

pub fn configured_mcp_process_supervisor_exe() -> Option<PathBuf> {
    MCP_PROCESS_SUPERVISOR_EXE.get().cloned()
}

/// Runs the internal MCP supervisor entry point and never returns.
pub fn run_mcp_process_supervisor_main() -> ! {
    let exit_code = run_mcp_process_supervisor().unwrap_or(1);
    std::process::exit(exit_code);
}

fn run_mcp_process_supervisor() -> io::Result<i32> {
    let mut args = std::env::args_os();
    let _argv0 = args.next();
    let _supervisor_flag = args.next();
    let program = args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "MCP process supervisor requires a target program",
        )
    })?;
    let parent_pid = unsafe { libc::getppid() };
    let process_group_id = unsafe { libc::getpgrp() };
    if parent_pid <= 1 || process_group_id <= 0 {
        return Err(io::Error::other(
            "MCP process supervisor requires a live parent and process group",
        ));
    }

    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    // The target inherits default signal handling. Ignore group-directed
    // graceful signals only in the supervisor so it can observe the target's
    // exit and remain available for escalation.
    unsafe {
        libc::signal(libc::SIGTERM, libc::SIG_IGN);
        libc::signal(libc::SIGINT, libc::SIG_IGN);
        libc::signal(libc::SIGHUP, libc::SIG_IGN);
        libc::close(libc::STDIN_FILENO);
        libc::close(libc::STDOUT_FILENO);
        libc::close(libc::STDERR_FILENO);
    }

    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status
                .code()
                .or_else(|| status.signal().map(|signal| 128 + signal))
                .unwrap_or(1));
        }
        if !parent_is_alive(parent_pid) {
            terminate_process_group(process_group_id);
        }
        std::thread::sleep(PARENT_POLL_INTERVAL);
    }
}

fn parent_is_alive(parent_pid: libc::pid_t) -> bool {
    if unsafe { libc::getppid() } != parent_pid {
        return false;
    }
    if unsafe {
        libc::kill(parent_pid, /*signal*/ 0)
    } == 0
    {
        return true;
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn terminate_process_group(process_group_id: libc::pid_t) -> ! {
    unsafe {
        libc::killpg(process_group_id, libc::SIGTERM);
    }
    std::thread::sleep(PROCESS_GROUP_TERM_GRACE_PERIOD);
    unsafe {
        libc::killpg(process_group_id, libc::SIGKILL);
        libc::_exit(1);
    }
}
