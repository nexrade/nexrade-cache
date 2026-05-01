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
use std::time::Duration;

use anyhow::{Context, Result};
use windows_service::{
    define_windows_service,
    service::{
        ServiceAccess, ServiceControl, ServiceControlAccept, ServiceErrorControl, ServiceExitCode,
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
pub fn install_service() -> Result<()> {
    let manager =
        ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CREATE_SERVICE)
            .context("open SCM (are you running as Administrator?)")?;

    let exe = std::env::current_exe().context("could not determine exe path")?;

    let info = ServiceInfo {
        name: OsString::from(SERVICE_NAME),
        display_name: OsString::from(SERVICE_DISPLAY),
        service_type: ServiceType::OWN_PROCESS,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: exe,
        launch_arguments: vec![OsString::from("--service")],
        dependencies: vec![],
        account_name: None, // LocalSystem
        account_password: None,
    };

    manager
        .create_service(&info, ServiceAccess::CHANGE_CONFIG)
        .context("create service")?
        .set_description(SERVICE_DESCRIPTION)
        .context("set description")?;

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

/// Entry point when the process is launched by the SCM (`--service` flag).
/// Hands control to the Windows service dispatcher which will call
/// [`ffi_service_main`] on a dedicated thread.
pub fn run_as_service() -> Result<()> {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
        .context("service dispatcher failed — is this process started by the SCM?")?;
    Ok(())
}

// The macro generates an `extern "system" fn ffi_service_main(...)` that the
// SCM dispatcher calls on a dedicated thread.
define_windows_service!(ffi_service_main, service_main);

fn service_main(_arguments: Vec<OsString>) {
    if let Err(e) = run_service() {
        eprintln!("service error: {e:#}");
    }
}

fn run_service() -> Result<()> {
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
    // remains available for SCM control events.
    let server_thread = std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(crate::run_server_default())
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

    // Report StopPending.
    let stop_pending = ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::StopPending,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 1,
        wait_hint: Duration::from_secs(5),
        process_id: None,
    };
    status_handle
        .set_service_status(stop_pending)
        .context("set StopPending")?;

    // Give the server thread a moment; it will exit when the tokio runtime
    // is dropped or when the TCP listener is closed.
    let _ = server_thread.join();

    // Report Stopped.
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

    Ok(())
}
