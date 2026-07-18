//! Windows Service Control Manager (SCM) integration.
//!
//! Allows nexrade-cache to be installed as a Windows service that starts
//! automatically at boot (ServiceStartType::AutoStart).
//!
//! # Usage
//!
//! ```cmd
//! REM Install (run as Administrator):
//! nexrade-cache --install-service
//!
//! REM Uninstall (run as Administrator):
//! nexrade-cache --uninstall-service
//! ```

#![cfg(windows)]

use std::ffi::OsString;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use nexrade_core::db::ServerConfig;
use windows_service::{
    define_windows_service,
    service::{
        ServiceAccess, ServiceAction, ServiceActionType, ServiceControl, ServiceControlAccept,
        ServiceErrorControl, ServiceExitCode, ServiceFailureActions, ServiceFailureResetPeriod,
        ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult},
    service_dispatcher,
    service_manager::{ServiceManager, ServiceManagerAccess},
};

const SERVICE_NAME: &str = "nexrade-cache";
const SERVICE_DISPLAY: &str = "Nexrade Cache";
const SERVICE_DESCRIPTION: &str = "High-performance Redis-compatible cache server (nexrade-cache)";

// ─── Install / Uninstall ──────────────────────────────────────────────────────

/// Install nexrade-cache as an auto-start Windows service.
/// Must be run with Administrator privileges.
///
/// `config_path` is the value the user passed via `--config` (if any) at
/// install time. It's baked into the service's launch arguments so every
/// SCM-triggered start — including after a reboot, when nobody is around to
/// pass `--config` again — loads the same config file instead of silently
/// falling back to defaults.
pub fn install_service(config_path: Option<&str>) -> Result<()> {
    let manager =
        ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CREATE_SERVICE)
            .context("open SCM (are you running as Administrator?)")?;

    let exe = std::env::current_exe().context("could not determine exe path")?;

    let mut launch_arguments = vec![OsString::from("--service")];
    if let Some(path) = config_path {
        // Resolve to an absolute path now, at install time, while we still
        // know the caller's working directory — the SCM always launches
        // services with an unrelated CWD (typically `C:\Windows\System32`),
        // so a relative path baked in here would silently fail to resolve.
        let abs = std::fs::canonicalize(path)
            .with_context(|| format!("config file not found: {path}"))?;
        launch_arguments.push(OsString::from("--config"));
        launch_arguments.push(abs.into_os_string());
    }

    let info = ServiceInfo {
        name: OsString::from(SERVICE_NAME),
        display_name: OsString::from(SERVICE_DISPLAY),
        service_type: ServiceType::OWN_PROCESS,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: exe,
        launch_arguments,
        dependencies: vec![],
        account_name: None, // LocalSystem
        account_password: None,
    };

    // `START` is required in addition to `CHANGE_CONFIG` because the failure
    // actions configured below include `SC_ACTION_RESTART` — SCM requires
    // the handle used to set that action to also hold `SERVICE_START`.
    let svc = manager
        .create_service(&info, ServiceAccess::CHANGE_CONFIG | ServiceAccess::START)
        .context("create service")?;
    svc.set_description(SERVICE_DESCRIPTION)
        .context("set description")?;

    // `ServiceStartType::AutoStart` only covers a full machine reboot — it
    // does nothing if the process itself dies while Windows keeps running
    // (panic, unhandled exception, etc). Without explicit failure actions,
    // SCM just leaves the service `Stopped` until someone notices. Restart
    // up to 3 times with a 5s delay, then give the failure counter an hour
    // to reset so a persistently-crashing binary doesn't restart forever.
    svc.update_failure_actions(ServiceFailureActions {
        reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(3600)),
        reboot_msg: None,
        command: None,
        actions: Some(vec![
            ServiceAction {
                action_type: ServiceActionType::Restart,
                delay: Duration::from_secs(5),
            },
            ServiceAction {
                action_type: ServiceActionType::Restart,
                delay: Duration::from_secs(5),
            },
            ServiceAction {
                action_type: ServiceActionType::Restart,
                delay: Duration::from_secs(5),
            },
        ]),
    })
    .context("set failure actions")?;
    // SCM only applies failure actions to crashes (access violations, etc.)
    // by default — a clean-but-nonzero process exit ("non-crash failure")
    // is ignored unless this is enabled. nexrade-cache exiting with an
    // error should also trigger the restart actions above.
    svc.set_failure_actions_on_non_crash_failures(true)
        .context("enable failure actions on non-crash exits")?;

    println!("Service '{}' installed successfully.", SERVICE_NAME);
    println!("Start it with:  sc start {}", SERVICE_NAME);
    Ok(())
}

/// Remove the nexrade-cache Windows service.
/// The service is stopped first if it is currently running.
/// Must be run with Administrator privileges.
pub fn uninstall_service() -> Result<()> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .context("open SCM (are you running as Administrator?)")?;

    let svc = manager
        .open_service(
            SERVICE_NAME,
            ServiceAccess::STOP | ServiceAccess::DELETE | ServiceAccess::QUERY_STATUS,
        )
        .context("open service — is it installed?")?;

    // Stop if running.
    let status = svc.query_status().context("query status")?;
    if status.current_state != ServiceState::Stopped {
        svc.stop().context("stop service")?;
        // Wait up to 5 s for it to reach Stopped.
        for _ in 0..50 {
            std::thread::sleep(Duration::from_millis(100));
            if svc.query_status()?.current_state == ServiceState::Stopped {
                break;
            }
        }
    }

    svc.delete().context("delete service")?;
    println!("Service '{}' removed.", SERVICE_NAME);
    Ok(())
}

// ─── Run as service ───────────────────────────────────────────────────────────

/// Config the service should start with, stashed here because the
/// `define_windows_service!`-generated entry point below has a fixed
/// `fn(Vec<OsString>)` signature (mandated by the SCM callback ABI) and can't
/// take extra parameters. `run_as_service` sets this immediately before
/// handing control to the dispatcher, so `service_main` always finds it
/// populated by the time the SCM invokes it.
static SERVICE_CONFIG: OnceLock<ServerConfig> = OnceLock::new();

/// Entry point when the process is launched by the SCM (`--service` flag).
/// Hands control to the Windows service dispatcher which will call
/// [`ffi_service_main`] on a dedicated thread.
///
/// `config` is the `ServerConfig` already resolved from `--config`/CLI flags
/// by the caller (same resolution path as a normal foreground run), so the
/// service honors the same config file on every SCM-triggered start.
pub fn run_as_service(config: ServerConfig) -> Result<()> {
    SERVICE_CONFIG
        .set(config)
        .map_err(|_| anyhow::anyhow!("run_as_service called more than once"))?;
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
        .context("service dispatcher failed — is this process started by the SCM?")?;
    Ok(())
}

// The macro generates an `extern "system" fn ffi_service_main(...)` that the
// SCM dispatcher calls on a dedicated thread.
define_windows_service!(ffi_service_main, service_main);

fn service_main(_arguments: Vec<OsString>) {
    // Populated by `run_as_service` just before the dispatcher call that
    // leads here; absent only if the SCM somehow invoked this entry point
    // without going through `run_as_service` first.
    let Some(config) = SERVICE_CONFIG.get() else {
        eprintln!("service error: no config set before service dispatch");
        return;
    };
    if let Err(e) = run_service(config.clone()) {
        eprintln!("service error: {e:#}");
    }
}

fn run_service(config: ServerConfig) -> Result<()> {
    // Channel used to signal the service to stop.
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();

    // Register the service control handler.  The SCM calls this closure when
    // it wants to stop/shutdown the service.
    let event_handler = move |control: ServiceControl| -> ServiceControlHandlerResult {
        match control {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                let _ = stop_tx.send(());
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    };

    let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)
        .context("register control handler")?;

    // Report: StartPending → Running.
    let pending = ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::StartPending,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 1,
        wait_hint: Duration::from_secs(10),
        process_id: None,
    };
    status_handle
        .set_service_status(pending)
        .context("set StartPending")?;

    // Start the tokio runtime + server on a background thread so this thread
    // remains available for SCM control events. The handle is intentionally
    // dropped (the thread runs detached): `start_server` -> `listener.run()`
    // loops for the life of the process and has no graceful-shutdown hook, so
    // there is nothing to join on at stop time — see the stop handling below.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(crate::start_server(config))
            .expect("server exited with error");
    });

    // Report Running.
    let running = ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::from_secs(0),
        process_id: None,
    };
    status_handle
        .set_service_status(running)
        .context("set Running")?;

    // Wait for stop signal from SCM.
    let _ = stop_rx.recv();

    // Report Stopped immediately. We do NOT try to join the server thread:
    // `listener.run()` never returns on its own, so joining here would hang
    // the service in STOP_PENDING forever (SCM then refuses both further
    // stops — error 1061 — and any restart — error 1056). Reporting Stopped
    // and then terminating the process is the correct shutdown for a service
    // whose workload has no cooperative-cancellation path: the OS reclaims
    // the listener sockets and tokio runtime on exit.
    let stopped = ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::from_secs(0),
        process_id: None,
    };
    status_handle
        .set_service_status(stopped)
        .context("set Stopped")?;

    // Terminate now that SCM has been told we're stopped. Returning would also
    // work (the detached server thread does not keep the process alive past
    // `main`), but an explicit exit removes any ambiguity and guarantees the
    // listener's port is released promptly for the next start.
    std::process::exit(0);
}
