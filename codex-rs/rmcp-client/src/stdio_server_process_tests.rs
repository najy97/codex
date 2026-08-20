use super::*;
use codex_exec_server::ExecProcessEventReceiver;
use codex_exec_server::ExecProcessFuture;
use codex_exec_server::ProcessId;
use codex_exec_server::ProcessSignal;
use codex_exec_server::ReadResponse;
use codex_exec_server::WriteResponse;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

struct FailingThenSuccessfulExecProcess {
    process_id: ProcessId,
    failures_before_success: usize,
    terminate_calls: AtomicUsize,
}

impl ExecProcess for FailingThenSuccessfulExecProcess {
    fn process_id(&self) -> &ProcessId {
        &self.process_id
    }

    fn subscribe_wake(&self) -> watch::Receiver<u64> {
        watch::channel(0).1
    }

    fn subscribe_events(&self) -> ExecProcessEventReceiver {
        ExecProcessEventReceiver::empty()
    }

    fn read(
        &self,
        _after_seq: Option<u64>,
        _max_bytes: Option<usize>,
        _wait_ms: Option<u64>,
    ) -> ExecProcessFuture<'_, ReadResponse> {
        Box::pin(async { unreachable!("cleanup test should not read process output") })
    }

    fn write(&self, _chunk: Vec<u8>) -> ExecProcessFuture<'_, WriteResponse> {
        Box::pin(async { unreachable!("cleanup test should not write process input") })
    }

    fn signal(&self, _signal: ProcessSignal) -> ExecProcessFuture<'_, ()> {
        Box::pin(async { unreachable!("cleanup test should not signal the process") })
    }

    fn terminate(&self) -> ExecProcessFuture<'_, ()> {
        let call = self.terminate_calls.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            if call <= self.failures_before_success {
                Err(codex_exec_server::ExecServerError::Protocol(format!(
                    "injected cleanup failure {call}"
                )))
            } else {
                Ok(())
            }
        })
    }
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn failed_cleanup_attempt_can_be_retried() {
    let handle = StdioServerProcessHandle::local(
        "invalid-process-group".to_string(),
        Some(LocalProcessTerminator::new(u32::MAX)),
    );

    for expected_attempt in 2..=3 {
        let error = handle
            .terminate()
            .await
            .expect_err("invalid process group must fail cleanup");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let cleanup = handle
            .inner
            .cleanup
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(cleanup.next_attempt, expected_attempt);
        assert!(matches!(&cleanup.status, ProcessCleanupStatus::Failed));
    }
}

#[tokio::test]
async fn bounded_cleanup_retry_succeeds_after_one_failure() {
    let process = Arc::new(FailingThenSuccessfulExecProcess {
        process_id: ProcessId::from("retry-cleanup"),
        failures_before_success: 1,
        terminate_calls: AtomicUsize::new(0),
    });
    let handle = StdioServerProcessHandle::executor(
        "retry-cleanup".to_string(),
        Arc::clone(&process) as Arc<dyn ExecProcess>,
    );

    handle
        .terminate_with_retry()
        .await
        .expect("second cleanup attempt should succeed");

    assert_eq!(process.terminate_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn bounded_cleanup_retry_reports_both_failures() {
    let process = Arc::new(FailingThenSuccessfulExecProcess {
        process_id: ProcessId::from("failed-cleanup"),
        failures_before_success: 2,
        terminate_calls: AtomicUsize::new(0),
    });
    let handle = StdioServerProcessHandle::executor(
        "failed-cleanup".to_string(),
        Arc::clone(&process) as Arc<dyn ExecProcess>,
    );

    let error = handle
        .terminate_with_retry()
        .await
        .expect_err("two failed cleanup attempts must be reported");

    assert!(error.to_string().contains("first attempt"));
    assert!(error.to_string().contains("retry"));
    assert_eq!(process.terminate_calls.load(Ordering::SeqCst), 2);
}

#[test]
fn executor_drop_without_a_runtime_still_terminates_the_process() {
    let process = Arc::new(FailingThenSuccessfulExecProcess {
        process_id: ProcessId::from("runtime-independent-drop"),
        failures_before_success: 0,
        terminate_calls: AtomicUsize::new(0),
    });
    let handle = StdioServerProcessHandle::executor(
        "runtime-independent-drop".to_string(),
        Arc::clone(&process) as Arc<dyn ExecProcess>,
    );

    drop(handle);

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while process.terminate_calls.load(Ordering::SeqCst) == 0
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(process.terminate_calls.load(Ordering::SeqCst), 1);
}
