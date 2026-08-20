#![cfg(unix)]

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use codex_rmcp_client::ElicitationAction;
use codex_rmcp_client::ElicitationResponse;
use codex_rmcp_client::LocalStdioServerLauncher;
use codex_rmcp_client::RmcpClient;
use futures::FutureExt as _;
use rmcp::model::ClientCapabilities;
use rmcp::model::Implementation;
use rmcp::model::InitializeRequestParams;
use rmcp::model::ProtocolVersion;
use serde_json::json;

fn stdio_server_bin() -> Result<std::path::PathBuf> {
    codex_utils_cargo_bin::cargo_bin("test_stdio_server").map_err(Into::into)
}

fn init_params() -> InitializeRequestParams {
    InitializeRequestParams::new(
        ClientCapabilities::default(),
        Implementation::new("codex-test", "0.0.0-test").with_title("Codex rmcp shutdown test"),
    )
    .with_protocol_version(ProtocolVersion::V_2025_06_18)
}

fn process_exists(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

async fn wait_for_pid_file(path: &Path) -> Result<u32> {
    for _ in 0..50 {
        match fs::read_to_string(path) {
            Ok(content) => {
                let trimmed = content.trim();
                if trimmed.is_empty() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }

                let pid = trimmed
                    .parse::<u32>()
                    .with_context(|| format!("failed to parse pid from {}", path.display()))?;
                return Ok(pid);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("failed to read {}", path.display()));
            }
        }
    }

    anyhow::bail!("timed out waiting for child pid file at {}", path.display());
}

async fn wait_for_process_exit(pid: u32) -> Result<()> {
    for _ in 0..50 {
        if !process_exists(pid) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!("process {pid} still running after timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervisor_reaps_term_resistant_server_after_parent_sigkill() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let server_pid_file = temp_dir.path().join("supervised-server.pid");
    let mut unowned = tokio::process::Command::new("/bin/sleep")
        .arg("300")
        .spawn()?;
    let unowned_pid = unowned.id().context("unowned process should have a pid")?;
    let mut parent = tokio::process::Command::new(stdio_server_bin()?)
        .env("MCP_TEST_SUPERVISOR_PARENT_ROLE", "1")
        .env("MCP_TEST_PID_FILE", &server_pid_file)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let parent_pid = parent.id().context("supervisor parent should have a pid")?;
    let server_pid = wait_for_pid_file(&server_pid_file).await?;
    assert!(process_exists(server_pid));

    let killed = std::process::Command::new("kill")
        .args(["-KILL", &parent_pid.to_string()])
        .status()?;
    assert!(killed.success(), "failed to SIGKILL supervisor parent");
    let _ = tokio::time::timeout(Duration::from_secs(5), parent.wait()).await??;
    wait_for_process_exit(server_pid).await?;
    assert!(
        process_exists(unowned_pid),
        "supervisor cleanup must preserve an unowned process group"
    );

    unowned.kill().await?;
    let _ = unowned.wait().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_stdio_server_initializes_and_shuts_down() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let server_pid_file = temp_dir.path().join("supervised-initialize.pid");
    let server_bin = stdio_server_bin()?;
    let client = RmcpClient::new_stdio_client(
        server_bin.clone().into_os_string(),
        Vec::new(),
        Some(HashMap::from([(
            OsString::from("MCP_TEST_PID_FILE"),
            OsString::from(server_pid_file.as_os_str()),
        )])),
        &[],
        /*cwd*/ None,
        Arc::new(
            LocalStdioServerLauncher::new(std::env::current_dir()?)
                .with_process_supervisor(Some(server_bin)),
        ),
    )
    .await?;
    client
        .initialize(
            init_params(),
            Some(Duration::from_secs(5)),
            Box::new(|_, _| async { unreachable!("test does not elicit") }.boxed()),
        )
        .await?;
    let server_pid = wait_for_pid_file(&server_pid_file).await?;

    client.try_shutdown().await?;

    wait_for_process_exit(server_pid).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn drop_kills_wrapper_process_group() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let child_pid_file = temp_dir.path().join("child.pid");
    let child_pid_file_str = child_pid_file.to_string_lossy().into_owned();

    let client = RmcpClient::new_stdio_client(
        OsString::from("/bin/sh"),
        vec![
            OsString::from("-c"),
            OsString::from(
                "sleep 300 & child_pid=$!; echo \"$child_pid\" > \"$CHILD_PID_FILE\"; cat >/dev/null",
            ),
        ],
        Some(HashMap::from([(
            OsString::from("CHILD_PID_FILE"),
            OsString::from(child_pid_file_str),
        )])),
        &[],
        /*cwd*/ None,
        Arc::new(LocalStdioServerLauncher::new(std::env::current_dir()?)),
    )
    .await?;

    let grandchild_pid = wait_for_pid_file(&child_pid_file).await?;
    assert!(
        process_exists(grandchild_pid),
        "expected grandchild process {grandchild_pid} to be running before dropping client"
    );

    drop(client);

    wait_for_process_exit(grandchild_pid).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_kills_initialized_stdio_server_with_in_flight_operation() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let server_pid_file = temp_dir.path().join("server.pid");
    let server_pid_file_str = server_pid_file.to_string_lossy().into_owned();

    let client = Arc::new(
        RmcpClient::new_stdio_client(
            stdio_server_bin()?.into(),
            Vec::<OsString>::new(),
            Some(HashMap::from([(
                OsString::from("MCP_TEST_PID_FILE"),
                OsString::from(server_pid_file_str),
            )])),
            &[],
            /*cwd*/ None,
            Arc::new(LocalStdioServerLauncher::new(std::env::current_dir()?)),
        )
        .await?,
    );

    client
        .initialize(
            init_params(),
            Some(Duration::from_secs(5)),
            Box::new(|_, _| {
                async {
                    Ok(ElicitationResponse {
                        action: ElicitationAction::Accept,
                        content: Some(json!({})),
                        meta: None,
                    })
                }
                .boxed()
            }),
        )
        .await?;

    let server_pid = wait_for_pid_file(&server_pid_file).await?;
    assert!(
        process_exists(server_pid),
        "expected MCP server process {server_pid} to be running before shutdown"
    );

    let call_client = Arc::clone(&client);
    let call_task = tokio::spawn(async move {
        call_client
            .call_tool(
                "sync".to_string(),
                Some(json!({ "sleep_after_ms": 300_000 })),
                /*meta*/ None,
                Some(Duration::from_secs(300)),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    client.shutdown().await;

    wait_for_process_exit(server_pid).await?;
    let _ = tokio::time::timeout(Duration::from_secs(5), call_task).await?;
    Ok(())
}

#[test]
fn second_shutdown_waits_for_shared_cleanup_before_runtime_exit() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let owned_pid_file = temp_dir.path().join("cancelled-shutdown.pid");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let owned_pid = runtime.block_on(async {
        let client = Arc::new(
            RmcpClient::new_stdio_client(
                OsString::from("/bin/sh"),
                vec![
                    OsString::from("-c"),
                    OsString::from(
                        "trap '' TERM; echo \"$$\" > \"$OWNED_PID_FILE\"; while :; do sleep 1; done",
                    ),
                ],
                Some(HashMap::from([(
                    OsString::from("OWNED_PID_FILE"),
                    OsString::from(owned_pid_file.as_os_str()),
                )])),
                &[],
                /*cwd*/ None,
                Arc::new(LocalStdioServerLauncher::new(std::env::current_dir()?)),
            )
            .await?,
        );
        let owned_pid = wait_for_pid_file(&owned_pid_file).await?;
        let mut unowned = tokio::process::Command::new("/bin/sleep")
            .arg("300")
            .spawn()?;
        let unowned_pid = unowned.id().context("unowned process should have a pid")?;

        let shutdown_client = Arc::clone(&client);
        let first_shutdown = tokio::spawn(async move { shutdown_client.shutdown().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        first_shutdown.abort();
        assert!(
            first_shutdown
                .await
                .expect_err("first shutdown caller should be cancelled")
                .is_cancelled()
        );

        let second_client = Arc::clone(&client);
        let mut second_shutdown =
            tokio::spawn(async move { second_client.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut second_shutdown)
                .await
                .is_err(),
            "second shutdown must wait for the shared SIGKILL cleanup"
        );
        tokio::time::timeout(Duration::from_secs(5), &mut second_shutdown)
            .await
            .context("second shutdown did not observe cleanup completion")??;
        assert!(
            !process_exists(owned_pid),
            "TERM-resistant MCP must be gone before second shutdown succeeds"
        );
        assert!(
            process_exists(unowned_pid),
            "cleanup must preserve a process outside the owned group"
        );
        unowned.kill().await?;
        let _ = unowned.wait().await?;
        Ok::<u32, anyhow::Error>(owned_pid)
    })?;

    drop(runtime);
    assert!(
        !process_exists(owned_pid),
        "MCP must remain gone after the Tokio runtime exits immediately"
    );
    Ok(())
}

async fn initialized_test_server(pid_file: &Path) -> Result<(Arc<RmcpClient>, u32)> {
    let client = Arc::new(
        RmcpClient::new_stdio_client(
            stdio_server_bin()?.into(),
            Vec::<OsString>::new(),
            Some(HashMap::from([(
                OsString::from("MCP_TEST_PID_FILE"),
                OsString::from(pid_file.as_os_str()),
            )])),
            &[],
            /*cwd*/ None,
            Arc::new(LocalStdioServerLauncher::new(std::env::current_dir()?)),
        )
        .await?,
    );
    client
        .initialize(
            init_params(),
            Some(Duration::from_secs(5)),
            Box::new(|_, _| async { unreachable!("test does not elicit") }.boxed()),
        )
        .await?;
    let server_pid = wait_for_pid_file(pid_file).await?;
    Ok((client, server_pid))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_timed_out_and_cancelled_calls_still_allow_shutdown() -> Result<()> {
    let failed_dir = tempfile::tempdir()?;
    let (failed_client, failed_pid) =
        initialized_test_server(&failed_dir.path().join("failed-call.pid")).await?;

    let failed_call = failed_client
        .call_tool(
            "missing-tool".to_string(),
            /*arguments*/ None,
            /*meta*/ None,
            Some(Duration::from_secs(1)),
        )
        .await;
    assert!(failed_call.is_err(), "unknown tool call should fail");
    failed_client.shutdown().await;
    assert!(!process_exists(failed_pid));

    let timeout_dir = tempfile::tempdir()?;
    let (timeout_client, timeout_pid) =
        initialized_test_server(&timeout_dir.path().join("timeout.pid")).await?;
    let timed_out_call = timeout_client
        .call_tool(
            "sync".to_string(),
            Some(json!({ "sleep_after_ms": 300_000 })),
            /*meta*/ None,
            Some(Duration::from_millis(25)),
        )
        .await;
    assert!(timed_out_call.is_err(), "slow tool call should time out");
    timeout_client.shutdown().await;
    assert!(!process_exists(timeout_pid));

    let cancelled_dir = tempfile::tempdir()?;
    let (cancelled_client, cancelled_pid) =
        initialized_test_server(&cancelled_dir.path().join("cancelled.pid")).await?;
    let call_client = Arc::clone(&cancelled_client);
    let call_task = tokio::spawn(async move {
        call_client
            .call_tool(
                "sync".to_string(),
                Some(json!({ "sleep_after_ms": 300_000 })),
                /*meta*/ None,
                Some(Duration::from_secs(300)),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    call_task.abort();
    assert!(
        call_task
            .await
            .expect_err("call task should be cancelled")
            .is_cancelled()
    );
    cancelled_client.shutdown().await;
    assert!(!process_exists(cancelled_pid));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialization_failure_then_shutdown_terminates_owned_server() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let server_pid_file = temp_dir.path().join("init-failure.pid");
    let client = RmcpClient::new_stdio_client(
        OsString::from("/bin/sh"),
        vec![
            OsString::from("-c"),
            OsString::from("echo \"$$\" > \"$SERVER_PID_FILE\"; cat >/dev/null"),
        ],
        Some(HashMap::from([(
            OsString::from("SERVER_PID_FILE"),
            OsString::from(server_pid_file.as_os_str()),
        )])),
        &[],
        /*cwd*/ None,
        Arc::new(LocalStdioServerLauncher::new(std::env::current_dir()?)),
    )
    .await?;
    let server_pid = wait_for_pid_file(&server_pid_file).await?;

    let initialization = client
        .initialize(
            init_params(),
            Some(Duration::from_millis(50)),
            Box::new(|_, _| async { unreachable!("test does not elicit") }.boxed()),
        )
        .await;
    assert!(
        initialization.is_err(),
        "non-MCP process should fail startup"
    );

    client.shutdown().await;
    assert!(!process_exists(server_pid));
    Ok(())
}
