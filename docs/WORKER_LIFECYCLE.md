# Жизненный цикл воркера (шпаргалка)

Как задача попадает в монитор, как её воркер запускается, меняется на ходу, как за ним следят и перезапускают.

## Участники

| Кто | Ответственность |
|---|---|
| `Orchestrator` | команды, агрегация в репо, события наружу, супервизия |
| `Supervisor` | владеет хендлами воркеров + backoff-состоянием (`HashMap<TaskId, WorkerRuntime>`) |
| `TaskRepository` | канон: спека + снапшот + метрики + статус |
| `PollWorker<A>` | расходный исполнитель: ритм → опрос → событие. Без стейта и команд |
| `UseCase::build` | асинхронная сборка адаптера (сетевой connect + SCN-резолюция) |

## Главный цикл Orchestrator

```rust
loop {
    select! {
        cmd     = cmd_rx.recv()             => команда (add/start/stop/update/remove/shutdown)
        ev      = events_rx.recv()          => воркер прислал результат — он ЖИВ
        outcome = build_rx.recv()           => адаптер собран (асинхронно, вне цикла)
        exit    = supervisor.next_exit()    => воркер ЗАВЕРШИЛСЯ сам (Completed/Failed/panic)
        _       = supervisor_tick.tick()    => каждые 1s: пора перезапускать (backoff истёк)
    }
}
```

Здоровье = один факт: **воркер шлёт события (жив) или завершился (exit-канал)**. Heartbeat не нужен.

---

## 1. Добавление новой задачи

### 1.1 Конфиг → валидация → `TaskSpec`

```
YAML / форма UI
  → DTO (сырые строки, serde)
  → AttemptConfig::try_new(...)      // timeout > 0
  → PollConfig::try_new(...)         // interval > 0, interval >= budget
  → QuerySnmpGet::from_raw(...)      // IpAddr, Community, SnmpOid, profile
  → TaskSpec::try_new(...)           // name непустой
```

Вся валидация — на этом шаге, **до** запуска. Невалидно → падаем сразу.

### 1.2 `add_task` — задача в `Idle` (воркера ещё нет)

```
Application::new(specs):
    run_id = ApplicationId::generate()
    Orchestrator::new() → tokio::spawn(orchestrator.run())
    для каждого spec → handle.add_task(spec)

add_task(spec):
    repository.add_task(spec) → TaskId, статус Idle
```

### 1.3 `start_task` — планируем сборку

```
start_task(id):
    если supervisor.is_running(id) || pending_builds.contains(id) → Ok (уже работает/собирается)
    если задачи нет → Err(TaskNotFound)
    supervisor.reset_restart(id)            // ручной старт сбрасывает накопленный backoff
    schedule_build(id, BuildIntent::Start)
```

### 1.4 `schedule_build` — сборка адаптера ВНЕ цикла

```
schedule_build(id, intent):
    если pending_builds.contains(id) → false (дубль)
    (query, attempt) = клон из спеки репо
    generation = build_generation[id]
    pending_builds.insert(id)
    supervisor.mark_building(id)            // снять таймер рестарта, attempts сохранить

    tokio::spawn:
        result = UseCase::build(query, attempt).await   // сеть: connect + SCN — НЕ блокирует цикл
        build_tx.send(BuildOutcome { id, intent, generation, result })
```

### 1.5 `handle_build_outcome` — спавним воркера

```
handle_build_outcome(outcome):
    pending_builds.remove(id)

    если generation != build_generation[id]:
        → спека успела поменяться, пока шла сборка → schedule_build ещё раз (пересборка)

    Ok(use_case):
        spawn_worker(id, use_case):
            (poll_config, metrics) = из репо (metrics — seed, переживают рестарт)
            supervisor.spawn(id, use_case, poll_config, metrics)   // см. §3
        статус Active, broadcast

    Err (intent == Start):
        → ручной старт: статус Idle, broadcast (НЕ ретраим)

    Err (intent == Rebuild):
        → supervisor.retry_later(id)   // attempts += 1, next_at = backoff
        → статус Restarting, broadcast
```

`Supervisor::spawn` (единственная точка запуска):

```
spawn(id, use_case, poll_config, metrics):
    worker = PollWorker::new(id, use_case, poll_config, events_tx, metrics)
    join = tokio::spawn:
        finished = catch_unwind(worker.run())   // паника → Failed("panic: ...")
        exit_tx.send((id, finished))
    runtimes[id].worker = Some(abort_handle)
    runtimes[id].restart.next_at = None          // таймер израсходован, attempts сохраняем
    // защита: если тут уже жил воркер — abort предыдущего
```

---

## 2. Изменение задачи на ходу (`update_task`)

```
update_task(id, new_spec):
    задачи нет → Err(TaskNotFound)
    repository.update_spec(id, new_spec)     // канон обновлён
    build_generation[id] += 1                // инвалидирует идущую сборку (гонка «конфиг поменяли, пока собирали»)

    если supervisor.is_running(id):
        supervisor.stop(id)                  // abort старого воркера (без exit-события)
        schedule_build(id, BuildIntent::Rebuild)   // пересборка из НОВОЙ спеки

    broadcast
```

Синхронизация = «**спека — канон; воркер перевыводится из неё**». Второго источника истины нет.

---

## 3. Мониторинг, смерть и перезапуск

### 3.1 Жизнь (воркер шлёт события)

```
PollWorker::run:
    loop {
        interval_tick.tick()
        если limit_reached (seed-метрики могли исчерпать лимит) → Completed
        poll(&attempt, &adapter):
            Success / NoResponse → metrics обновить → events_tx.send(WorkerEvent{id, metrics, result})
            Err(Fatal) → Failed(message)
        если limit_reached → Completed
    }
```

Оркестратор на `WorkerEvent`:

```
handle_worker_event(ev):
    supervisor.reset_restart(id)   // воркер ПЕРЕЖИЛ опрос → сброс backoff
    статус = (limit > 0 && metrics.total_attempts >= limit) ? RatedLimit : Active
    repository.update_snapshot(id, snapshot)
    broadcast
```

### 3.2 Смерть (exit-канал)

```
handle_worker_exit(id, finished):
    Completed (лимит / приёмник упал):
        supervisor.mark_stopped(id)   → статус RatedLimit, НЕ перезапускаем
    Failed (Fatal / паника):
        schedule_restart(id):
            worker = None
            attempts += 1
            next_at = now + backoff(attempts)   // 1s, 2s, 4s, 8s, 16s, 32s, 60s (cap)
        статус Restarting, broadcast, log error
```

### 3.3 Перезапуск (по тику, а не сразу)

```
supervisor_tick (1s):
    due = supervisor.due_restarts(now)   // next_at <= now
    для каждого id → restart_task(id)

restart_task(id):
    schedule_build(id, BuildIntent::Rebuild)   // НЕ сбрасываем attempts — backoff нарастает
```

Сброс backoff происходит **только** в двух местах:
1. воркер пережил опрос (`handle_worker_event` → `reset_restart`);
2. ручной старт (`start_task` → `reset_restart`).

`restart_task` **не** сбрасывает — поэтому краш-луп нарастает: 1s → 2s → 4s → … → 60s.

---

## Статусы (`PollStatus`)

| Статус | Когда | Перезапускаем? |
|---|---|---|
| `Idle` | добавлен, не стартовал; или ручной старт упал на сборке | по команде `start` |
| `Active` | воркер работает | — |
| `Paused` | `stop_task` | по команде `start` |
| `RatedLimit` | дошёл до `limit` | по команде `start` (но seed-метрики сразу исчерпают лимит) |
| `Restarting` | упал, ждёт backoff | да, по тику |

---

## Важные факты (gotchas)

1. **`abort()` не шлёт exit.** `stop`/`remove`/`update` гасят воркер тихо; exit-событие приходит только когда воркер завершился **сам**.
2. **Сборка адаптера — асинхронная и вне цикла.** Сетевой connect/SCN не блокирует команды и события.
3. **`generation` защищает от гонки**: если спека обновилась, пока шла сборка — результат отбрасывается и пересобирается.
4. **Метрики — seed**: при пересоздании берутся из репо, поэтому счётчики/латентность не сбрасываются.
5. **`retries` = число повторов после первой попытки** (итого `1 + retries`).
6. **`interval >= budget`** — жёсткое правило (`budget = timeout*(retries+1) + retry_delay*retries`).

---

## Компактная схема

```
добавление:  config → валидация → TaskSpec → add_task(Idle) → start_task
             → schedule_build(Start) → build(вне цикла) → spawn → Active

изменение:   update_task → update_spec + generation++ → stop(abort) → schedule_build(Rebuild)
             → build → spawn (из новой спеки) → Active

жизнь:       воркер шлёт WorkerEvent → snapshot в репо + broadcast + reset_restart

смерть:      exit-канал:
               Completed  → RatedLimit (стоп)
               Failed     → Restarting + next_at = backoff(attempts)

перезапуск:  tick(1s) → next_at истёк → schedule_build(Rebuild) → build → spawn → Active
             (build упал → retry_later: attempts++, ждём дальше)
```
