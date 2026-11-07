use chrono::Local;
use tokio::{
    sync::broadcast,
    time::{Duration, MissedTickBehavior, interval},
};

use tools_core::{
    DT_FMT,
    monitor::{
        event::MonitorEvent,
        runtime::{ApplicationHandle, MonitorSubscription, error::OrchestratorError},
        task::MonitorSnapshot,
    },
};

use super::render::{LINE_DOUBLE_LN, format_snapshot};

pub async fn run(
    app: &ApplicationHandle,
    subscription: MonitorSubscription,
) -> Result<(), OrchestratorError> {
    let MonitorSubscription {
        mut snapshot,
        mut events,
    } = subscription;
    let monitor_id = app.id().clone();
    let app_created_at = Local::now();
    let app_created_at_fmt = app_created_at.format(DT_FMT);
    let mut refresh = interval(Duration::from_millis(100));
    refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        clear_screen();
        let uptime = Local::now() - app_created_at;
        println!(
            "{LINE_DOUBLE_LN}Monitor ID: {monitor_id}\nUptime: {}m {}s. Started: {app_created_at_fmt}\n{LINE_DOUBLE_LN}\n{}",
            uptime.num_minutes(),
            uptime.num_seconds() % 60,
            format_snapshot(&snapshot.tasks),
        );

        if !wait_for_update(&snapshot, &mut events).await {
            return Ok(());
        }
        refresh.tick().await;
        snapshot = app.get_snapshot().await?;
    }
}

async fn wait_for_update(
    snapshot: &MonitorSnapshot,
    events: &mut broadcast::Receiver<MonitorEvent>,
) -> bool {
    loop {
        match events.recv().await {
            Ok(event)
                if event.metadata.source != snapshot.source
                    || event.metadata.seq >= snapshot.next_seq =>
            {
                return true;
            }
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(_)) => return true,
            Err(broadcast::error::RecvError::Closed) => return false,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tools_core::{
        monitor::{
            probe::SnmpGetSpec,
            runtime::{AppConfig, Application},
            scenario::{PollSchedule, Scenario, SnmpGetScenarioSpec},
            task::TaskConfig,
        },
        polling::AttemptConfig,
    };

    fn task(name: &str) -> TaskConfig {
        let request =
            SnmpGetSpec::from_raw("127.0.0.1".to_string(), 161, "public".to_string(), None, vec![])
                .unwrap();
        let attempt = AttemptConfig::try_new(Duration::from_millis(10), 0, Duration::ZERO).unwrap();
        let scenario = Scenario::SnmpGet(
            SnmpGetScenarioSpec::try_new(
                request,
                PollSchedule::FixedDelay {
                    delay: Duration::from_secs(1),
                },
                attempt,
            )
            .unwrap(),
        );
        TaskConfig::try_new(name.to_string(), scenario, 0, 0).unwrap()
    }

    #[tokio::test]
    async fn subscription_updates_skip_events_already_in_the_snapshot() {
        let running = Application::new(AppConfig::default(), vec![]).spawn();
        let app = running.handle();
        let mut events = app.subscribe();
        app.add_task(task("first")).await.unwrap();
        let snapshot = app.get_snapshot().await.unwrap();
        app.add_task(task("second")).await.unwrap();

        assert!(wait_for_update(&snapshot, &mut events).await);
        assert!(matches!(events.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_updates_finish_after_covered_events_and_channel_closure() {
        let running = Application::new(AppConfig::default(), vec![]).spawn();
        let app = running.handle();
        let mut events = app.subscribe();
        app.add_task(task("first")).await.unwrap();
        let snapshot = app.get_snapshot().await.unwrap();
        running.shutdown().await.unwrap();

        assert!(!wait_for_update(&snapshot, &mut events).await);
    }

    #[tokio::test]
    async fn subscription_updates_request_refresh_after_lagging() {
        let running = Application::new(AppConfig::default(), vec![]).spawn();
        let app = running.handle();
        let mut source_events = app.subscribe();
        let (sender, mut events) = broadcast::channel(1);
        for name in ["first", "second", "third"] {
            app.add_task(task(name)).await.unwrap();
            sender.send(source_events.recv().await.unwrap()).unwrap();
        }
        let snapshot = app.get_snapshot().await.unwrap();
        drop(sender);

        assert!(wait_for_update(&snapshot, &mut events).await);
        assert!(!wait_for_update(&snapshot, &mut events).await);
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_updates_request_refresh_when_the_source_changes() {
        let running = Application::new(AppConfig::default(), vec![]).spawn();
        let app = running.handle();
        app.add_task(task("first")).await.unwrap();
        let snapshot = app.get_snapshot().await.unwrap();
        let other_running = Application::new(AppConfig::default(), vec![]).spawn();
        let other = other_running.handle();
        let mut events = other.subscribe();
        other.add_task(task("other")).await.unwrap();

        assert!(wait_for_update(&snapshot, &mut events).await);
        running.shutdown().await.unwrap();
        other_running.shutdown().await.unwrap();
    }
}
