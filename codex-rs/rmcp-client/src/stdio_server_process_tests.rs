use super::*;

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
