# Спецификация: архитектура сетевого опроса и монитора

> Статус: прототип, актуально под текущий код.
> Область: подсистема «сетевые запросы + живой монитор» крейтов `tools-core` и `tctl`.
> Центральная идея: **независимые, переиспользуемые компоненты, собираемые под задачу**.

---

## 1. Цель и проблема

Транспортному инженеру постоянно приходится работать с **новым** оборудованием: появляется
новый дорожный контроллер, вендор добавляет новый OID, нужно быстро проверить значение,
затестировать устройство или сравнивать несколько устройств бок о бок.

Протоколы разные (SNMP сейчас; далее HTTP, Modbus, ICMP-ping и т.д.), а **оркестрация одинакова**:
периодичность, таймауты, ретраи, метрики, история, отображение.

Отсюда два требования:

1. **Разделить** «что спросить и как понять ответ» (протокол) от «когда спросить и как управлять»
   (оркестрация).
2. **Сделать компоненты независимыми**, чтобы их можно было собирать в комбинации под задачу
   без переписывания ядра.

---

## 2. Идея

Есть набор независимых примитивов: **клиенты** (snmp/http/modbus/…), **адаптер-юскейс**
(реализует `Pollable`), **воркер**, **task/repository**, **Orchestrator**. Любой, кто реализует
`Pollable`, передаётся воркеру: воркер периодически вызывает `poll` и шлёт результат в канал.

`Orchestrator` — **единственный источник правды** по задачам и их воркерам: владеет
репозиторием и хендлами воркеров, принимает команды через один канал, слушает события,
пересоздаёт упавшие воркеры (supervisor) и рассылает обновления. `Application` — тонкий
клиент поверх него: читает конфиг, готовит `TaskSpec` и шлёт команды.

Модель данных — **Spec / Status / Control**:

- **Spec** (желаемое): `TaskSpec` внутри `TaskEntity` (`name + query + poll_config + deep_history`).
- **Status** (фактическое): `snapshot` + `history` в `TaskEntity`.
- **Control** (управление): хендлы воркеров и счётчики рестартов — только в `Orchestrator`.

---

## 3. Глоссарий

| Термин | Смысл |
|---|---|
| **Клиент** | Обёртка над протоколом: `SnmpReadClient`, далее `HttpClient`, `ModbusClient`. Знает протокол, не знает расписание/задачи. |
| **Use-case / адаптер** | Тип запроса: `snmp-get` (реализован), далее `http-read`, `modbus-read`, `snmp-set`, `ping`. Реализует `Pollable`. |
| **`Pollable`** | Трейт адаптера: `async fn poll(&self) -> Result<Output, AttemptError>` с ассоциированным `type Output: Send`. |
| **`UseCase`** | Закрытый enum адаптеров (`SnmpGet(SnmpReader)`, …). Сам реализует `Pollable`, объединяя все адаптеры в один тип. |
| **`UseCaseOutput`** | Закрытый enum результатов адаптера (`SnmpGet(SnmpGetResponse)`, …). |
| **`UseCaseQuery`** | Закрытый enum запроса; хранит валидированные доменные типы (`IpAddr`, `Community`, `SnmpOid`). |
| **`TaskSpec`** | Валидированная спека (Spec): `name + query + poll_config + deep_history`; приватные поля, `try_new`. |
| **Worker** | Stateless исполнитель: ритм опроса, ретраи, метрики, события. Без mailbox и машины состояний. Протокол-агностичен. |
| **`WorkerHandle`** | Ручка остановки воркера: `abort()`, `id()`. |
| **Orchestrator** | Контроль + супервизор: владеет хендлами воркеров и backoff-рестартами; команды/события/broadcast. |
| **`OrchestratorHandle`** | Пульт оркестратора: `add_task`, `remove_task`, `start/stop/update_task`, `get_snapshot`, `subscribe`. |
| **Task** | Логическая задача мониторинга = спека + расписание + история. |
| **Snapshot** | Снимок состояния задачи (результат + метрики + статус). |
| **Repository** | In-memory хранилище задач и их снапшотов. |
| **Application** | Тонкий клиент: конфиг (валидация в `TaskSpec`) + обёртка над `OrchestratorHandle`. |
| **`Response<T>`** | Итог одной итерации опроса: `Success { payload, … }` / `NoResponse { … }` — оба штатные (value). |
| **`AttemptError`** | Ошибка одной попытки: `Transient` (ретраится) / `Fatal` (не ретраится). |
| **`FatalError`** | Фатальная ошибка итерации — единственный возможный `Err` из `poll()`. |
| **`WorkerFinished`** | Итог завершения воркера: `Completed` / `Failed`. |
| **SCN** | Site Code Number — ASCII-идентификатор контроллера, встраиваемый в OID (`.1.<len>.<bytes>`). |
| **Profile** | Вендор/протокол контроллера (Swarco, PotokS, PotokUg405, …). |
| **Stage / фаза / такт** | Текущая фаза светофорного объекта. |

---

## 4. Принципы проектирования

1. **Независимость компонентов.** Клиент, адаптер, воркер, task, repository — самодостаточные
   единицы; собираются в цепочку под задачу.
2. **`Application` — тонкий клиент, а не обязательный путь.** Низкоуровневые сценарии
   (проверка OID, тест устройства) собираются напрямую из примитивов, минуя `Application`.
3. **Core не зависит от UI.** Вывод/форматирование живут в `tctl` (`tools-cli/src/monitor/formatters/`).
4. **Worker не знает протоколов.** Параметризован только `Pollable`; generic по `A::Output`.
5. **Закрытые enum'ы вместо `dyn` в ядре.** `UseCase`/`UseCaseOutput`/`Response<T>` — закрытые
   множества; `dyn` допустим только на внешних точках расширения (например, history-sink).
6. **Оркестрация написана один раз.** Таймауты/ретраи/метрики/история — в core, не в адаптерах.
7. **Адаптер отвечает за интерпретацию.** Сырые байты → `BusinessValue` делает адаптер;
   воркер и UI получают нормализованные данные.
8. **Сборка адаптера — в фабрике (`UseCase::build`), а не в Orchestrator'е.** Orchestrator
   остаётся протокол-агностичным.
9. **Spec/Status/Control.** Конфигурация (`spec`) и фактическое состояние (`snapshot`) живут
   в `TaskEntity` (данные); хендлы/счётчики рестартов — только в `Orchestrator` (control).
   Воркер — расходный исполнитель, восстанавливается из Spec+Status.
10. **Supervisor через жизненный цикл.** Изменение конфигурации или падение воркера
    обрабатывается пересозданием (`spawn`/`abort`), а не командами в живой воркер.

---

## 5. Действующие лица и сущности

### Компоненты (действующие лица)

| Компонент | Роль | Ключевые типы |
|---|---|---|
| **CLI** (`tctl`) | входная точка: парсит YAML, запускает, рендерит | `main.rs`, `AppBuilder`, formatters |
| **Application** | конфиг + тонкий клиент: валидация конфига → `TaskSpec`, отправка команд | `Application`, `ApplicationId`, `ApplicationState` |
| **Orchestrator** | контроль + супервизор: хендлы воркеров, backoff-рестарты | `Orchestrator`, `OrchestratorHandle`, `OrchestratorCommand`, `OrchestratorEvent` |
| **PollWorker** | stateless исполнитель: ритм опроса, ретраи, метрики | `PollWorker<A>`, `WorkerHandle`, `WorkerEvent` |
| **UseCase (адаптер)** | «один опрос → типизированный результат» | `UseCase`, `UseCaseOutput`, `UseCaseQuery` |
| **TaskRepository** | in-memory хранилище состояния задач | `TaskRepository`, `TaskEntity`, `TaskSnapshot`, `TaskHistory` |

### Сущности (данные)

| Сущность | Что хранит |
|---|---|
| `TaskSpec` | `name + query + poll_config + deep_history` — Spec (желаемое) |
| `TaskEntity` | `spec` (Spec) + `snapshot`/`history` (Status) + created/updated |
| `TaskSnapshot` | `poll_result + metrics + poll_status` на момент времени |
| `Metrics` | счётчики попыток и латентность (total/success/errors, current/min/max) |
| `Response<UseCaseOutput>` | итог опроса: `Success` / `NoResponse` |
| `WorkerEvent<T>` | факты воркера: `{ id, metrics, result: Response<T> }` |

---

## 6. Организация кода

Код разбит по **зонам ответственности** (функционалу), а не по слоям: каждый модуль владеет своим функционалом целиком — протокол, опрос, оркестрация, конфиг — и зависит от нижележащих модулей только через их публичный API.

```
                 ┌──────────────────────────────────────────────┐
                 │               UI (tctl / будущий web)         │
                 │   подписка, форматирование, вывод             │
                 └──────────────────┬───────────────────────────┘
                                    │ broadcast: OrchestratorEvent
┌───────────────────────────────────▼────────────────────────────────────────────┐
│                               tools-core                                       │
│                                                                                │
│  monitor::application                                                         │
│   Application (тонкий клиент) ── команды ──► Orchestrator ── владеет ──► TaskRepository
│        │  валидация конфига            ▲   (хендлы + backoff-рестарты)  (spec+snapshot)
│        │                               │                                     │
│        │                    WorkerEvent (events канал)                        │
│        │                               │                                     │
│  ┌─────▼───────────────────────────────┴──────┐                             │
│  │ polling::worker                            │                             │
│  │   PollWorker<UseCase> (stateless)          │                             │
│  │     outbox (events_tx), WorkerHandle       │                             │
│  └─────┬──────────────────────────────────────┘                             │
│        │ poll(&use_case) + метрики/ретраи/таймауты                           │
│  ┌─────▼──────────────────────────────┐                                      │
│  │ UseCase (адаптер, enum)            │  SnmpGet(SnmpReader), …              │
│  └─────┬──────────────────────────────┘                                      │
│  ┌─────▼──────────────────────────────┐                                      │
│  │ snmp (протокол)                    │  client, value, oid, profile,        │
│  │                                     │  registry, parsers                  │
│  └────────────────────────────────────┘                                      │
└────────────────────────────────────────────────────────────────────────────────┘
```

Правило зависимостей: модуль зависит только от нижележащего модуля; `polling` не знает про
`snmp`/`use-case`; `Orchestrator` оперирует `UseCase` (enum), не зная протокольных деталей.

---

## 7. Поток данных

### 7.1 Старт (создание задач)

```
YAML (в tctl) → DTO (serde) → TaskSpec::try_from (валидация: try_new-конструкторы, без I/O)
     → Application::new(Vec<TaskSpec>):
         Orchestrator::new() → (orchestrator, handle); tokio::spawn(orchestrator.run())
         handle.add_task(spec) — на каждую спеку
     → Orchestrator.handle_command(AddTask):
         TaskRepository.add_task(spec)  → task_id   (задача в Idle, без коннекта)
```

### 7.2 Запуск

```
Application::start()
  → handle.start_task(task_id) для каждой задачи
  → Orchestrator.handle_command(StartTask):
      статус Starting → schedule_build (async, вне цикла):
        UseCase::build(query, attempt) → BuildOutcome → build_rx
      handle_build_outcome → spawn_worker → Supervisor::spawn:
        PollWorker::new(worker_id, use_case, poll_config, events_tx, seed_metrics)
        tokio::spawn(catch_unwind(run)) → WorkerHandle (abort)
```

### 7.3 Цикл опроса (горячий путь)

```
интервал сработал
  → PollWorker.run:
      poll(&attempt, &use_case) — ретраит только Transient
      адаптер: устройство → сырые данные → UseCaseOutput
      → Response::Success { payload, attempts, errors, elapsed }
         Response::NoResponse { attempts, errors, elapsed }   (все попытки Transient)
         Err(FatalError) → WorkerFinished::Failed
      метрики обновляются (min/max агрегирует репо)
  → WorkerEvent { id, metrics, result: Response<UseCaseOutput> }
  → events_tx (mpsc) → Orchestrator.handle_worker_event:
      poll_status = f(metrics vs limit): Active | RatedLimit
      TaskSnapshot → TaskRepository.update_snapshot
      broadcast OrchestratorEvent::TaskUpdated { task_id, view: TaskView }
  → UI (tctl): get_snapshot (база) + subscribe (дельты) → format_snapshot
```

### 7.4 Горячее управление

```
Start / Stop / Update:
  handle.start_task / stop_task / update_task
  → OrchestratorCommand → Orchestrator

StartTask:   build use_case → spawn_worker (если ещё не запущена)
StopTask:    worker_control.abort() → poll_status = Paused
UpdateTask:  TaskRepository.update_spec(spec) → пересоздать воркера (если запущена)

RemoveTask:
  Orchestrator: worker_control.abort() + TaskRepository.remove_task
  → возвращает удалённую TaskEntity (oneshot)
```

### 7.5 Супервизия (реализовано)

```
воркер завершился сам → exit-канал (catch_unwind → (task_id, WorkerFinished)):
  Completed (лимит)    — poll_status = RatedLimit, не рестартуем
  Failed(msg) / паника — Restarting + backoff (1s/2s/4s/…/60s) + broadcast
  abort() (наш stop)   — exit-события НЕ шлёт (тихий)

supervisor_tick (1s):
  для задач с истёкшим restart.next_at:
    schedule_build(Rebuild) → build → spawn_worker (seed-метрики из snapshot) → Active + broadcast
```

---

## 8. Взаимодействие между компонентами

Каналы связи (все — tokio-каналы):

| От → Кому | Канал | Что передаёт |
|---|---|---|
| Application → Orchestrator | `mpsc<OrchestratorCommand>` + `oneshot` (ответ) | AddTask/RemoveTask/StartTask/StopTask/UpdateTask/GetSnapshot/Subscribe |
| Worker → Orchestrator | `mpsc<WorkerEvent>` (общий, fan-in) | факты опросов: metrics, poll_result |
| Orchestrator → UI | `broadcast<OrchestratorEvent>` | снапшоты (обновления) |
| Orchestrator → history-sink (будущее) | `mpsc<HistoryRecord>` (надёжный, не broadcast) | записи истории |

Правила:

- **Команды — запрос-ответ** там, где нужен результат (`add_task` возвращает `TaskId`,
  `start_task`/`update_task` — `Result`, `remove_task` — `TaskEntity`, `get_snapshot`/`subscribe` — через `oneshot`).
- **События воркеров — fan-in** в один `events`-канал оркестратора.
- **Воркер не получает команд** — управление через жизненный цикл (`spawn`/`abort`).
- **UI получает lossy `broadcast`** (отстающий подписчик теряет промежуточные кадры — для экрана ок).
- **История (если появится) — отдельный lossless `mpsc`** + writer-актор, чтобы медленный диск
  не тормозил горячий путь и данные не терялись.

---

## 9. Модель воркера (stateless)

```rust
// Адаптер: классифицирует природу ошибки одной попытки
#[async_trait]
pub trait Pollable: Send + Sync {
    type Output: Send;
    async fn poll(&self) -> Result<Self::Output, AttemptError>;
}

pub enum AttemptError {
    Transient(String),   // сеть/таймаут — ретраится
    Fatal(String),       // баг/конфиг — не ретраится
}

// Итог итерации опроса (value): оба исхода штатные
pub enum Response<T> {
    Success { timestamp, attempts, errors, elapsed, payload: T },
    NoResponse { timestamp, attempts, errors, elapsed },
}

// Воркер — generic, без mailbox/команд/FSM
pub struct PollWorker<A: Pollable> { /* id, poll_config, metrics, adapter, event_tx, tick */ }

impl<A: Pollable> PollWorker<A> {
    pub fn new(id, adapter, poll_config, event_tx: Sender<WorkerEvent<A::Output>>, metrics: Metrics) -> Self;
    pub async fn run(self) -> WorkerFinished;   // Completed | Failed
}

// Ручка остановки
pub struct WorkerHandle { abort: tokio::task::AbortHandle }

// Факты воркера
pub struct WorkerEvent<T> { pub id: WorkerId, pub metrics: Metrics, pub result: Response<T> }
pub enum WorkerFinished { Completed, Failed(String) }
```

Воркер опрашивает, пока `total_attempts < limit`; при достижении лимита завершается
`Completed`. Фатальная ошибка (`AttemptError::Fatal`) → `Failed`. Остановка — `abort()`;
изменение конфига — пересоздание.

---

## 10. Точки расширения: рецепт добавления нового use-case

1. **Адаптер** — новый тип с `impl Pollable` (образец: `snmp/adapters/reader.rs`).
2. **`UseCase`** — новый вариант + arm в `poll()`.
3. **`UseCaseOutput`** — новый вариант.
4. **`UseCaseQuery`** — новый вариант + DTO в `tctl` (serde) + arm в `target()`; валидация — в `from_raw`-конструкторе.
5. **`UseCase::build`** — arm, собирающий адаптер из валидированного query.
6. **Форматтер** — ветка в `match resp.payload` (`tools-cli/.../formatters/repository.rs`).

---

## 11. Контракты интерфейсов (сводка)

```rust
// расписание и попытка
pub struct AttemptConfig { pub timeout: Duration, pub retries: u8, pub retry_delay: Duration }
pub struct PollConfig  { pub interval: Duration, pub limit: u64, pub attempt: AttemptConfig }

// ошибки polling
pub enum AttemptError { Transient(String), Fatal(String) }
pub struct FatalError { pub message: String }

// итог итерации опроса (value)
pub enum Response<T> {
    Success { timestamp, attempts, errors, elapsed, payload: T },
    NoResponse { timestamp, attempts, errors, elapsed },
}

// один опрос с ретраями (в core, один раз): ретраит Transient, Fatal -> Err
pub async fn poll<A: Pollable>(config: &AttemptConfig, adapter: &A)
    -> Result<Response<A::Output>, FatalError>;

// итог завершения воркера
pub enum WorkerFinished { Completed, Failed(String) }

// метрики
pub struct Metrics { /* total/success/errors, current/min/max latency */ }
```

---

## 12. Обработка ошибок

- **`AttemptError`** (`polling`) — ошибка одной попытки: `Transient` (ретраится) / `Fatal` (не ретраится).
- **`FatalError`** (`polling`) — фатальная ошибка итерации, единственный `Err` из `poll()`.
- **`SnmpError`** (`snmp`) — протокол: `Network`/`Timeout` (transient), `Auth`/`Protocol`/`InvalidOid`/парсерные (fatal).
- **`ParseError`** (`snmp`) — парсинг значения в доменный тип.
- **`AsciiError`** (`ascii`) — работа с ASCII/SCN-строками.
- **`QueryError`** (`monitor::task`) — валидация запроса в `QuerySnmpGet::from_raw`.
- **`TaskError`** / **`TaskRepositoryError`** (`monitor::task`) — валидация задачи / операции репозитория.
- **`UseCaseBuildError`** (`monitor::usecase`) — сборка адаптера в `UseCase::build`.
- **`OrchestratorError`** (`monitor::orchestrator`) — команды/оркестрация (`Build`, `TaskRepository`, `TaskNotFound`, `ChannelClosed`).
- **`ConfigError`** (`polling`) — валидация `PollConfig`/`AttemptConfig`.

Все они сходятся в корневой `error::Error` — тонкий зонт из `#[from]`.

Правило: ошибка интерпретации одного OID **не роняет** весь опрос — в `SnmpReader` она
превращается в `BusinessValue::Text("parse error")`, остальные OID доставляются.

---

## 13. Открытые вопросы

- **O-1. `UseCase`/`UseCaseOutput`/`Response` — закрытые enum'ы.** Ок для своих use-case'ов;
  `dyn` — только на внешних точках расширения (history-sink и т.п.).
- **O-2. Лимит рестартов.** Сейчас backoff бесконечный (до cap 60s). Нужен ли `max_restarts`
  + статус `Failed` для задачи, падающей детерминированно.
- **O-3. Graceful shutdown.** ✅ сделано: команда `Shutdown` + `Drop for Supervisor` (abort всех воркеров).
- **O-4. Семантика `limit`.** ✅ сделано: `limit` считается **за запуск** (ручной `start` сбрасывает
  счётчики через `reset_metrics`); рестарт после падения / `update` сохраняют.
- **O-5. `Query` vs `UseCaseQuery`.** ✅ сделано: один набор — `UseCaseQuery` хранит валидированные
  доменные типы (`IpAddr`/`Community`/`SnmpOid`), DTO (сырые строки) — только в `tctl`.
- **O-6. Персистентность истории.** `TaskHistory` в памяти; нужен ли history-sink (файл/sqlite)
  через отдельный writer-актор + `mpsc`.
- **O-7. Автокорреляция между задачами.** Группировка уже есть; автоматическое сравнение
  снапшотов — опционально.
- **O-8. Границы «клиент vs адаптер».** Подключение/сериализация/декодирование — зафиксировать.

---

## 14. Дорожная карта

> Живой список задач (со статусами) — в [`docs/ROADMAP.md`](ROADMAP.md).

1. **History-sink** (O-6): `LogWriter` trait (`dyn`) + writer-актор + `mpsc`.
2. **Второй use-case** (http-read или icmp-ping) — проверить рецепт §10.
3. **snmp-set** — отдельный «командный» путь (не периодический опрос).
4. ✅ **Свести `Query`/`UseCaseQuery`** (убрать дубли, доменные типы в query) — сделано.
5. **Лимит рестартов + статус `Failed`** (O-2) и graceful shutdown (O-3).
6. **Web-интерфейс** поверх `OrchestratorHandle` (subscribe уже есть).
7. **Параллельно**: самостоятельные инструменты `tools-core` (расчёты фаз/циклов/тактов,
   конвертеры, парсеры логов) — чистые функции, вне акторной модели.

---

## 15. Связанные файлы (карта)

| Компонент | Файл |
|---|---|
| Трейт адаптера | `crates/tools-core/src/polling/pollable.rs` |
| Опрос/расписание | `crates/tools-core/src/polling/{poll.rs, config.rs, metrics.rs, error.rs}` |
| Воркер | `crates/tools-core/src/polling/worker/{worker.rs, types.rs}` |
| UseCase (адаптер/фабрика) | `crates/tools-core/src/monitor/usecase/use_case.rs` |
| Orchestrator | `crates/tools-core/src/monitor/orchestrator/orchestrator.rs` |
| Application (тонкий клиент) | `crates/tools-core/src/monitor/application/app.rs` |
| Задачи/репозиторий/view | `crates/tools-core/src/monitor/task/{spec.rs, query.rs, entity.rs, id.rs, repository.rs, view.rs, error.rs}` |
| SNMP-адаптер/клиент | `crates/tools-core/src/snmp/{adapters/reader.rs, client.rs, error.rs}` |
| Профили/реестр/парсеры | `crates/tools-core/src/snmp/{profiles.rs, registry/, parsers/}` |
| SCN/ASCII | `crates/tools-core/src/ascii.rs` |
| Stage (фаза) | `crates/tools-core/src/stage.rs` |
| CLI | `crates/tools-cli/src/{main.rs, monitor/app.rs, monitor/formatters/}` |

## 16. Схема 

        ┌──────────────────────────────────────────────────────┐
входы ──┤  cmd_rx: Command                                     │
        │  facts_rx: WorkerEvent<UseCaseOutput>                │
        │  exit_rx: (TaskId, WorkerFinished)                   │
        │  build_rx: BuildOutcome                              │
        │  tick: Interval(1s)                                  │
        └───────────────┬──────────────────────────────────────┘
                        ▼
                    RUNTIME.run  ──▶  apply(msg)  ──▶  Vec<MonitorEvent>
                        │                                   │
        ┌───────────────┴───────────────┐                   ▼
        │ live: broadcast<MonitorEvent> │   durable: mpsc<MonitorEvent>
        └───────────────────────────────┘
