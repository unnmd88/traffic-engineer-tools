use std::collections::BTreeMap;

use chrono::Local;
use tokio::sync::broadcast;

use tools_core::{
    DT_FMT,
    monitor::{
        event::MonitorEvent,
        runtime::Application,
        task::{TaskId, TaskView},
    },
};

use super::render::{LINE_DOUBLE_LN, format_snapshot};

/// Подписка на события + живой вывод дашборда.
pub async fn run(app: Application, mut rx: broadcast::Receiver<MonitorEvent>) {
    let monitor_id = app.id().clone();
    let app_created_at = Local::now();
    let app_created_at_fmt = Local::now().format(DT_FMT);

    // События больше не несут полный view — таблица строится из снапшота
    // (событие служит триггером перерисовки).
    while rx.recv().await.is_ok() {
        let snapshot = app.get_snapshot().await.expect("Failed to get snapshot");
        let mut dashboard: BTreeMap<TaskId, TaskView> = BTreeMap::new();
        for t in snapshot.tasks {
            dashboard.insert(t.id, t);
        }

        clear_screen();
        let uptime = Local::now() - app_created_at;
        println!(
            "{LINE_DOUBLE_LN}Monitor ID: {monitor_id}\nUptime: {}m {}s. Started: {app_created_at_fmt}\n{LINE_DOUBLE_LN}\n{}",
            uptime.num_minutes(),
            uptime.num_seconds() % 60,
            format_snapshot(&dashboard),
        );
    }
}

#[cfg(not(target_os = "windows"))]
fn clear_screen() {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(b"\x1b[H\x1b[2J\x1b[3J");
    let _ = out.flush();
}

#[cfg(target_os = "windows")]
fn clear_screen() {
    use crossterm::{
        cursor::MoveTo,
        execute,
        terminal::{Clear, ClearType},
    };
    use std::io::stdout;
    let _ = execute!(stdout(), Clear(ClearType::All), MoveTo(0, 0));
}
