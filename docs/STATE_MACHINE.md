# Памятка: проектирование и реализация автомата (state machine)

> Для подсистемы мониторинга `tools-core`. Живой пример — `TaskStatus` + `Orchestrator`.
> Это не спецификация, а шпаргалка «как думать и как писать код».

---

## 1. Что такое состояние — два компаса

**Компас 1 — «что есть состояние».** Состояние = то, что система **помнит между
событиями**, чтобы принять следующее решение. Если это не нужно помнить для решения —
это данные, а не состояние (метрики, история, backoff — данные).

**Компас 2 — «состояние vs событие».** Если что-то происходит **мгновенно** и между
этими моментами система «в нём» не находится — это **событие**, а не состояние.

Проверка на нашем коде:

- `Starting` — состояние (сборка адаптера длится, задача всё это время «в Starting»).
- `Polled` — **не** состояние (опрос мгновенный; до и после задача `Active`). Поэтому
  `Polled` живёт в `ChangeKind`, а не в `TaskStatus`.

---

## 2. Рецепт (порядок работы)

Идём **не** от «придумаем enum», а от триггеров:

1. **Триггеры** — список всего, что может случиться (видно из API и из устройства).
2. **Нарратив** — рассказать историю объекта вслух, собирая состояния.
3. **Граф** — стрелки `состояние ──событие──▶ состояние` (на бумаге/в комментарии).
4. **`enum` + `match`** — инкрементально: сначала happy path, потом грани.
5. **Тесты** — на хитрые переходы (рестарт, stale-build, идемпотентность), а не на все.

Таблица переходов — это **сжатая запись графа**, а не первый шаг. Она нужна, когда
переходов много; на 6 состояниях — опционально.

---

## 3. State и Transition — на примере монитора

### `State` — «что помним» (данные, без каналов/таймеров)

```rust
struct State {
    tasks: HashMap<TaskId, TaskEntity>,          // то, что уже есть в Orchestrator
    next_id: u64,
    workers: HashMap<TaskId, WorkerSlot>,        // «жив ли воркер» + backoff (без AbortHandle)
    pending_builds: HashSet<TaskId>,
}
```

Это просто поля-данные из `Orchestrator`, вынесенные отдельно, чтобы их можно было
тестировать без tokio.

### `Transition` — результат одного шага

```rust
struct Transition {
    effects: Vec<Effect>,          // «что СДЕЛАТЬ в реальном мире» (спавн/аборт)
    event: Option<MonitorEvent>,   // «что СООБЩИТЬ наружу»
}
```

Это тот же результат, что раньше возвращал `apply` (`Option<MonitorEvent>`), только
плюс `effects` — «поручения», которые чистая функция не выполняет сама.

### Это не конвейер, это цикл

```
цикл {
    сообщение = взять из каналов;
    результат = transition(state, сообщение);   // машина ПОДУМАЛА
    выполнить результат.effects;                 // сделать порученное
    разослать результат.event;                   // сообщить об изменении
}
```

### Две ветки

**Без effects** (ничего спавнить не надо) — «пришёл опрос»:

```rust
Msg::Facts(f) => {
    state.tasks.get_mut(&id)?.apply_poll(/* ... */);
    Transition { effects: vec![], event: Some(TaskChanged { kind: Polled, .. }) }
}
```

**С effect** (надо поднять воркера) — «адаптер собрался»:

```rust
Msg::Built(out) => {
    state.tasks.get_mut(&out.task_id)?.set_status(Active);
    Transition {
        effects: vec![Effect::SpawnWorker { task_id, use_case, poll_config, metrics }],
        event: Some(TaskChanged { kind: Restarted, .. }),
    }
}
```

Цикл исполняет записку:

```rust
let t = transition(&mut self.state, msg);
for eff in t.effects {
    match eff {
        Effect::SpawnWorker { .. } => { /* реальный tokio::spawn */ }
        // ...
    }
}
```

---

## 4. Таблица переходов (результат для `TaskStatus`)

| Триггер | Переход | `ChangeKind` |
|---|---|---|
| `AddTask` | — | `Added` |
| `StartTask` | → `Starting` | `Starting` |
| `on_built` успех | `Starting` → `Active` | `Started` |
| `on_built` успех | `Restarting` → `Active` | `Restarted` |
| `on_built` ошибка | → `Restarting` | `BuildFailed { reason }` |
| `on_facts` | `Active` → `Active` | `Polled` |
| `on_exit` `Completed` | → `RatedLimit` | `Completed` |
| `on_exit` `Failed` | → `Restarting` | `Failed { reason }` |
| `StopTask` | → `Stopped` | `Stopped` |
| `UpdateTask` (не запущена) | статус прежний | `SpecUpdated` |
| `UpdateTask` (запущена) | → `Restarting` | `SpecUpdated` |
| `RemoveTask` | — | отдельное `TaskRemoved` |

`on_built` различает `Started`/`Restarted` **по статусу перед `Active`**
(`Restarting` → `Restarted`, иначе → `Started`) — без `BuildIntent`.

---

## 5. Правила

1. **Не пытайся сразу идеально.** Начни с 3–4 очевидных состояний; `Starting`/`Restarting`
   добавь, когда код покажет, что «сборка идёт, а статуса нет».
2. **Состояния взаимоисключающие и полные:** объект всегда ровно в одном из них.
   Если два «флага» могут быть одновременно — это **не состояния**, а ортогональные
   поля (`pending_builds`, backoff) — держи их отдельно.
3. **Событие ≠ состояние** (компас 2). Не плоди `TaskStatus::Polled`/`Crashed` — это события.
4. **Недопустимый переход делай явным** (`None` в таблице или `warn!`), а не тихим `_ => {}`.

---

## Typestate: когда НЕ подходит

Typestate («состояние в типе, методы возвращают новый тип») красив для **линейных**
потоков (builder, request→response). Для рантайм-автомата не подходит:

1. состояние меняется **асинхронно из каналов**, а не через вызовы методов;
2. жизненный цикл **цикличный** (`Active → Restarting → Active → …`);
3. статус нужен **как данные** (показать/сравнить/сериализовать).

Дух «невалидное невыразимо» у нас уже применён где надо — в `try_new`-валидации данных
(`TaskSpecPayload`, `PollConfig`, `AttemptConfig`).

---

## 6. Тестирование: с выносом State или без

Два пути, оба валидны.

### 6.1 Без выноса `State` (проще, `#[tokio::test]`)

Тестируем текущий `Orchestrator::apply` целиком, без рефакторинга:

```rust
#[tokio::test]
async fn failed_worker_goes_restarting() {
    let (kernel, api) = Orchestrator::new(None);
    tokio::spawn(kernel.run());

    let id = api.add_task(spec()).await.unwrap();
    api.start_task(id).await.unwrap();
    // … дать воркеру упасть/завершиться, затем:
    let snap = api.get_snapshot().await.unwrap();
    assert_eq!(snap.tasks[0].status, TaskStatus::Restarting);
}
```

Плюс: покрывает и каналы, и spawn, и порядок. Минус: нужен рантайм, тест чуть
медленнее и «интеграционный».

### 6.2 С выносом `State` (unit-тесты чистой `transition`)

См. раздел 3. Плюс: быстрые исчерпывающие тесты переходов без tokio. Минус:
плата за `effects` + два представления воркера (чистый `WorkerSlot` + `WorkerHandle`).

**Рекомендация:** начинать с 6.1 (`#[tokio::test]`); выносить `State` (6.2), только
когда интеграционные тесты станут громоздкими.
