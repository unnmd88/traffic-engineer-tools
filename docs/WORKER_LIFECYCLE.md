# Супервизия воркеров (шпаргалка)

Как Orchestrator запускает воркеры, следит за их здоровьем и перезапускает.

## Роли

- **Orchestrator** = мозг: принимает команды, строит адаптер, пишет в репо, рассылает UI.
- **Supervisor** = руки: владеет хендлами воркеров и backoff-таймерами.
- **repository** = память: канон задачи (спека + снапшот + метрики).

## Главный цикл Orchestrator

```rust
loop {
    select! {
        cmd   = cmd_rx.recv()              => /* команда: start/stop/update/remove */
        ev    = events_rx.recv()           => /* воркер прислал результат — он ЖИВ */
        exit  = supervisor.next_exit()     => /* воркер ЗАВЕРШИЛСЯ САМ — он УМЕР */
        _     = supervisor_tick.tick()     => /* каждые 1s: пора перезапускать */
    }
}
```

Здоровье = один факт: **жив воркер или завершился**.

## Состояние (в Supervisor)

```rust
runtimes: HashMap<TaskId, WorkerRuntime>
WorkerRuntime {
    worker:  Option<WorkerHandle>,   // Some = запущен, None = остановлен/упал/ждёт рестарт
    restart: RestartState { attempts: u32, next_at: Option<Instant> },
}
```

Наличие `worker` и есть «запущен/не запущен».

## 1. Запуск

Единственная точка — `Supervisor::spawn`. К ней сводятся `start_task`, `restart_task`, `update_task`.

```
start_task(id):
    если supervisor.is_running(id) → no-op
    иначе build_use_case(id) → spawn_worker(id, use_case)

spawn_worker(id, use_case):
    seed из репо (poll_config + metrics) → supervisor.spawn(id, use_case, config, metrics)

Supervisor::spawn:
    создать PollWorker
    tokio::spawn(обёртка): run → catch_unwind → лог → exit_tx.send((id, finished))
    сохранить handle (abort) в runtimes[id].worker
```

## 2. Жизнь (мониторинг здоровья)

| Сигнал | Значит | Обработчик |
|---|---|---|
| `WorkerEvent` из `events_rx` | жив, опрос выполнен | обновить snapshot + broadcast |
| `(id, WorkerFinished)` из exit-канала | завершился **сам** | разобрать причину |

Воркер не шлёт heartbeat — его живость = он ещё шлёт события.

## 3. Смерть: разбор причины

`handle_worker_exit(task_id, finished)`:

| `finished` | Что делаем |
|---|---|
| `Completed` (дошёл до `limit`) | `mark_stopped` → статус `RatedLimit`, не перезапускаем |
| `Failed` (фатальная ошибка / паника) | `schedule_restart` → `Restarting` + broadcast |

**Логирование**: `Failed` → `tracing::error!(..., "worker failed ({message}), scheduling restart")`; `Completed` → `tracing::info!(..., "worker completed")`. Плюс сам факт завершения задачи логируется в обёртке `Supervisor::spawn` (`worker task finished`).

## 4. Перезапуск (backoff)

`schedule_restart` НЕ перезапускает сразу — только планирует:

```
schedule_restart(id):
    worker = None
    attempts += 1
    next_at = now + backoff(attempts)   // 1s, 2s, 4s, ... до 60s
```

Перезапуск — по тику:

```
supervisor_tick (каждые 1s):
    due = supervisor.due_restarts(now)   // next_at <= now
    для каждого id → restart_task

restart_task(id):
    supervisor.reset_restart(id)         // next_at = None, attempts = 0
    build_use_case(id):
        Ok  → spawn_worker → Active → broadcast
        Err → supervisor.retry_later(id) // attempts += 1, next_at = backoff
```

## Цепочка «умер → пересоздали» (по шагам)

```
t=0    воркер паникует
         → обёртка ловит панику → exit_tx.send((id, Failed("panic: ...")))
         → Orchestrator: handle_worker_exit(id, Failed)
         → schedule_restart: attempts=1, next_at=t+1s
         → статус Restarting, broadcast, log error
t=1s   tick → due_restarts вернул [id]
         → restart_task: build → spawn нового воркера (seed метрик из репо)
         → статус Active, broadcast
         (если build упал → retry_later: next_at=t+2s, остаёмся Restarting)
t=2s   ... пробуем снова
```

## Команда «изменить конфиг» (update_task)

```
update_task(id, new_spec):
    1) задача есть? нет → Err(TaskNotFound)
    2) repository.update_spec(id, new_spec)   // канон в репо (interval/limit/query)
    3) если supervisor.is_running(id):
         supervisor.stop(id)                  // abort старого
         build_use_case → spawn_worker        // новый воркер из новой спеки
    4) broadcast
```

Метрики сохраняются: `spawn_worker` берёт их из репо как seed.

## Важно: abort НЕ шлёт exit

`abort()` отменяет tokio-задачу насильно — обёртка не доходит до `exit_tx.send`.

Поэтому `handle_worker_exit` вызывается **только когда воркер завершился сам** (`Completed`/`Failed`/паника), а не когда мы его `abort`'нули (`stop`/`remove`/`update`).

Итог:
- **abort** (наш `stop`) → тихо, без exit-события;
- **самозавершение** → exit-событие → реагируем.

## Компактная схема

```
запуск:    start_task → spawn_worker (seed из репо)
жизнь:     воркер шлёт события → snapshot обновляется
смерть:    exit-канал → Completed | Failed
           ├─ Completed → RatedLimit (стоп)
           └─ Failed/panic → Restarting + next_at (жди backoff)
перезапуск: tick(1s) → next_at истёк → build → spawn → Active
```

## Откуда может прийти паника

Воркер (`polling`) сам не паникует. Паника приходит из:

1. **`adapter.poll()` → `async_snmp`** — внешняя библиотека.
2. **Парсеры сырых значений** (`snmp/value.rs`: `v[0]..v[3]` для `IpAddress` и т.п.) — malformed-данные от устройства.

`catch_unwind` (только в `Supervisor::spawn`) — страховка: паника убивает только этот воркер, не весь супервизор.
