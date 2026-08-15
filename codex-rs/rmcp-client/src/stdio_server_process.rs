//! Ownership and termination of processes spawned for stdio MCP transports.

use std::io;
#[cfg(windows)]
use std::os::windows::io::OwnedHandle;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
#[cfg(unix)]
use std::thread::sleep;
#[cfg(unix)]
use std::thread::spawn;
use std::time::Duration;

use codex_exec_server::ExecProcess;
#[cfg(all(unix, not(target_os = "macos")))]
use codex_utils_pty::process_group::kill_process_group;
#[cfg(target_os = "macos")]
use codex_utils_pty::process_group::kill_process_group_with_member_fallback as kill_process_group;
#[cfg(all(unix, not(target_os = "macos")))]
use codex_utils_pty::process_group::process_group_exists;
#[cfg(target_os = "macos")]
use codex_utils_pty::process_group::process_group_exists_with_member_fallback as process_group_exists;
#[cfg(all(unix, not(target_os = "macos")))]
use codex_utils_pty::process_group::terminate_process_group;
#[cfg(target_os = "macos")]
use codex_utils_pty::process_group::terminate_process_group_with_member_fallback as terminate_process_group;
use tokio::sync::watch;
use tracing::warn;

#[cfg(unix)]
const PROCESS_GROUP_TERM_GRACE_PERIOD: Duration = Duration::from_secs(2);
#[cfg(unix)]
const PROCESS_GROUP_EXIT_POLL_INTERVAL: Duration = Duration::from_millis(25);
#[cfg(unix)]
const PROCESS_GROUP_KILL_CONFIRM_PERIOD: Duration = Duration::from_secs(1);
const EXECUTOR_TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(unix)]
/// Exact process-group ownership token captured when the stdio server is spawned.
pub(crate) struct LocalProcessTerminator {
    process_group_id: u32,
}

#[cfg(windows)]
/// Exact Windows ownership token captured when the stdio server is spawned.
///
/// A job owns the complete process tree. The process-handle variant is the
/// fallback when a remote executor cannot provide a job object.
pub(crate) enum LocalProcessTerminator {
    Job(codex_utils_pty::JobObject),
    Process(OwnedHandle),
}

#[cfg(not(any(unix, windows)))]
/// Placeholder on targets without local process-tree termination support.
pub(crate) struct LocalProcessTerminator;

/// Shared owner for one spawned stdio MCP process tree.
///
/// Every clone coordinates through one cleanup state. Termination always uses
/// the ownership token captured at spawn time; it never discovers targets by
/// scanning process IDs or executable names.
#[derive(Clone)]
pub(crate) struct StdioServerProcessHandle {
    inner: Arc<StdioServerProcessHandleInner>,
}

struct StdioServerProcessHandleInner {
    program_name: String,
    kind: StdioServerProcessKind,
    cleanup: Mutex<ProcessCleanupState>,
}

struct ProcessCleanupState {
    next_attempt: u64,
    status: ProcessCleanupStatus,
}

/// `Idle | Failed -> Running -> Complete | Failed`.
///
/// All callers observing `Running` wait on the same completion receiver. A
/// failed attempt remains retryable, while a completed attempt is idempotent.
enum ProcessCleanupStatus {
    Idle,
    Running {
        attempt: u64,
        completion: watch::Receiver<Option<ProcessCleanupOutcome>>,
    },
    Complete,
    Failed,
}

type ProcessCleanupOutcome = Result<(), ProcessCleanupFailure>;

#[derive(Clone)]
struct ProcessCleanupFailure {
    kind: io::ErrorKind,
    message: String,
}

enum StdioServerProcessKind {
    Local(Option<LocalProcessTerminator>),
    Executor(Arc<dyn ExecProcess>),
}

impl LocalProcessTerminator {
    #[cfg(not(windows))]
    pub(crate) fn new(process_group_id: u32) -> Self {
        #[cfg(unix)]
        {
            Self { process_group_id }
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = process_group_id;
            Self
        }
    }

    #[cfg(unix)]
    async fn terminate(&self) -> io::Result<()> {
        let process_group_id = self.process_group_id;
        match terminate_process_group(process_group_id) {
            Ok(false) => return Ok(()),
            Ok(true) => {
                let term_deadline = tokio::time::Instant::now() + PROCESS_GROUP_TERM_GRACE_PERIOD;
                match Self::wait_for_exit(process_group_id, term_deadline).await {
                    Ok(true) => return Ok(()),
                    Ok(false) => {}
                    Err(error) => {
                        warn!(
                            "Failed to confirm MCP process group {process_group_id} after SIGTERM: {error}"
                        );
                    }
                }
            }
            Err(error) => {
                warn!("Failed to terminate MCP process group {process_group_id}: {error}");
            }
        }

        let kill_error = kill_process_group(process_group_id).err();
        let kill_deadline = tokio::time::Instant::now() + PROCESS_GROUP_KILL_CONFIRM_PERIOD;
        match Self::wait_for_exit(process_group_id, kill_deadline).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(kill_error.unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "MCP process group {process_group_id} still exists after SIGKILL confirmation timeout"
                    ),
                )
            })),
            Err(error) => Err(kill_error.unwrap_or(error)),
        }
    }

    #[cfg(unix)]
    async fn wait_for_exit(
        process_group_id: u32,
        deadline: tokio::time::Instant,
    ) -> io::Result<bool> {
        loop {
            if !process_group_exists(process_group_id)? {
                return Ok(true);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(PROCESS_GROUP_EXIT_POLL_INTERVAL.min(deadline - now)).await;
        }
    }

    #[cfg(windows)]
    async fn terminate(&self) -> io::Result<()> {
        let result = match self {
            Self::Job(job) => job.terminate(),
            Self::Process(process_handle) => {
                codex_utils_pty::JobObject::terminate_process_handle(process_handle)
            }
        };
        result.map_err(io::Error::other)
    }

    #[cfg(not(any(unix, windows)))]
    async fn terminate(&self) -> io::Result<()> {
        Ok(())
    }

    #[cfg(unix)]
    fn terminate_on_drop(&self) {
        let process_group_id = self.process_group_id;
        let should_escalate = match terminate_process_group(process_group_id) {
            Ok(exists) => exists,
            Err(error) => {
                warn!("Failed to terminate MCP process group {process_group_id}: {error}");
                true
            }
        };
        if should_escalate {
            spawn(move || {
                sleep(PROCESS_GROUP_TERM_GRACE_PERIOD);
                if let Err(error) = kill_process_group(process_group_id) {
                    warn!("Failed to kill MCP process group {process_group_id}: {error}");
                }
            });
        }
    }

    #[cfg(not(unix))]
    fn terminate_on_drop(&self) {
        #[cfg(windows)]
        {
            let result = match self {
                Self::Job(job) => job.terminate(),
                Self::Process(process_handle) => {
                    codex_utils_pty::JobObject::terminate_process_handle(process_handle)
                }
            };
            if let Err(error) = result {
                warn!("Failed to terminate Windows MCP process: {error}");
            }
        }
    }
}

impl StdioServerProcessHandle {
    pub(crate) fn local(program_name: String, terminator: Option<LocalProcessTerminator>) -> Self {
        Self {
            inner: Arc::new(StdioServerProcessHandleInner {
                program_name,
                kind: StdioServerProcessKind::Local(terminator),
                cleanup: Mutex::new(ProcessCleanupState::new()),
            }),
        }
    }

    pub(crate) fn executor(program_name: String, process: Arc<dyn ExecProcess>) -> Self {
        Self {
            inner: Arc::new(StdioServerProcessHandleInner {
                program_name,
                kind: StdioServerProcessKind::Executor(process),
                cleanup: Mutex::new(ProcessCleanupState::new()),
            }),
        }
    }

    /// Starts or joins cleanup for the exact process tree owned by this handle.
    pub(crate) async fn terminate(&self) -> io::Result<()> {
        let (attempt, completion, start_cleanup) = {
            let mut cleanup = self
                .inner
                .cleanup
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match &cleanup.status {
                ProcessCleanupStatus::Complete => return Ok(()),
                ProcessCleanupStatus::Running {
                    attempt,
                    completion,
                } => (*attempt, completion.clone(), None),
                ProcessCleanupStatus::Idle | ProcessCleanupStatus::Failed => {
                    let attempt = cleanup.next_attempt;
                    cleanup.next_attempt = cleanup.next_attempt.wrapping_add(1);
                    let (finished, completion) = watch::channel(None);
                    cleanup.status = ProcessCleanupStatus::Running {
                        attempt,
                        completion: completion.clone(),
                    };
                    (attempt, completion, Some(finished))
                }
            }
        };

        if let Some(finished) = start_cleanup {
            let inner = Arc::clone(&self.inner);
            // The task owns cleanup independently of the initiating caller. If
            // the caller is cancelled, later callers still join this attempt;
            // if the runtime drops the task, dropping `inner` activates the
            // runtime-independent local fallback below.
            std::mem::drop(tokio::spawn(async move {
                let outcome = inner
                    .terminate_owned_process()
                    .await
                    .map_err(ProcessCleanupFailure::from);
                inner.finish_cleanup_attempt(attempt, outcome.clone());
                finished.send_replace(Some(outcome));
            }));
        }

        self.inner.wait_for_cleanup(attempt, completion).await
    }
}

impl ProcessCleanupState {
    fn new() -> Self {
        Self {
            next_attempt: 1,
            status: ProcessCleanupStatus::Idle,
        }
    }
}

impl From<io::Error> for ProcessCleanupFailure {
    fn from(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

impl ProcessCleanupFailure {
    fn into_error(self) -> io::Error {
        io::Error::new(self.kind, self.message)
    }
}

impl StdioServerProcessHandleInner {
    async fn terminate_owned_process(&self) -> io::Result<()> {
        match &self.kind {
            StdioServerProcessKind::Local(Some(terminator)) => terminator.terminate().await,
            StdioServerProcessKind::Local(None) => Ok(()),
            StdioServerProcessKind::Executor(process) => {
                terminate_executor_process(process, &self.program_name).await
            }
        }
    }

    fn finish_cleanup_attempt(&self, attempt: u64, outcome: ProcessCleanupOutcome) {
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(
            &cleanup.status,
            ProcessCleanupStatus::Running {
                attempt: running_attempt,
                ..
            } if *running_attempt == attempt
        ) {
            cleanup.status = if outcome.is_ok() {
                ProcessCleanupStatus::Complete
            } else {
                ProcessCleanupStatus::Failed
            };
        }
    }

    async fn wait_for_cleanup(
        &self,
        attempt: u64,
        mut completion: watch::Receiver<Option<ProcessCleanupOutcome>>,
    ) -> io::Result<()> {
        loop {
            if let Some(outcome) = completion.borrow().clone() {
                return outcome.map_err(ProcessCleanupFailure::into_error);
            }
            if completion.changed().await.is_err() {
                self.finish_cleanup_attempt(
                    attempt,
                    Err(ProcessCleanupFailure {
                        kind: io::ErrorKind::BrokenPipe,
                        message: "MCP process cleanup task ended without a result".to_string(),
                    }),
                );
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "MCP process cleanup task ended without a result",
                ));
            }
        }
    }
}

async fn terminate_executor_process(
    process: &Arc<dyn ExecProcess>,
    program_name: &str,
) -> io::Result<()> {
    match tokio::time::timeout(EXECUTOR_TERMINATION_TIMEOUT, process.terminate()).await {
        Ok(result) => result.map_err(io::Error::other),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "executor MCP server termination timed out after {} seconds ({program_name})",
                EXECUTOR_TERMINATION_TIMEOUT.as_secs()
            ),
        )),
    }
}

impl Drop for StdioServerProcessHandleInner {
    fn drop(&mut self) {
        if matches!(
            &self
                .cleanup
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner)
                .status,
            ProcessCleanupStatus::Complete
        ) {
            return;
        }

        match &self.kind {
            StdioServerProcessKind::Local(Some(terminator)) => terminator.terminate_on_drop(),
            StdioServerProcessKind::Local(None) => {}
            StdioServerProcessKind::Executor(process) => {
                let process = Arc::clone(process);
                let program_name = self.program_name.clone();
                let Ok(handle) = tokio::runtime::Handle::try_current() else {
                    warn!(
                        "Could not schedule remote MCP server process termination on drop ({}): no Tokio runtime is available",
                        self.program_name
                    );
                    return;
                };

                std::mem::drop(handle.spawn(async move {
                    if let Err(error) = terminate_executor_process(&process, &program_name).await {
                        warn!(
                            "Failed to terminate remote MCP server process on drop ({program_name}): {error}"
                        );
                    }
                }));
            }
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
#[path = "stdio_server_process_tests.rs"]
mod tests;
