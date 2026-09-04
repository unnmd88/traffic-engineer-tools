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
(реализует `Pollable`), **воркер-актор**, **task/repository**, **Orchestrator**. Любой, кто
реализует `Pollable`, передаётся воркеру: воркер периодически вызывает `poll`, шлёт результат
в канал и принимает команды.

`Orchestrator` — **единственный источник правды** по задачам и их воркерам: владеет
репозиторием, хендлами воркеров и маппингом, принимает команды через один канал и рассылает
обновления подписчикам. `Application` — тонкий клиент поверх него: читает конфиг, готовит
`TaskSpec` и шлёт команды.

Сочетание паттернов:

1. **Ports & Adapters (hexagonal)** — core не зависит от транспорта; протоколы за адаптерами.
2. **Actor model + Supervisor** — воркеры как акторы, Orchestrator как супервизор.
3. **Закрытые enum'ы** — `UseCase` (адаптеры), `UseCaseOutput` (результаты), `PollResult` (итог)
   как единый «язык» между воркером, репозиторием и UI.

---

## 3. Глоссарий

| Термин | Смысл |
|---|---|
| **Клиент** | Обёртка над протоколом: `SnmpReadClient`, далее `HttpClient`, `ModbusClient`. Знает протокол, не знает расписание/задачи. |
| **Use-case / адаптер** | Тип запроса: `snmp-get` (реализован), далее `http-read`, `modbus-read`, `snmp-set`, `ping`. Реализует `Pollable`. |
| **`Pollable`** | Трейт адаптера: `async fn poll(&self) -> Result<Output, PollError>` с ассоциированным `type Output: Send`. |
| **`UseCase`** | Закрытый enum адаптеров (`SnmpGet(SnmpReader)`, …). Сам реализует `Pollable`, объединяя все адаптеры в один тип. |
| **`UseCaseOutput`** | Закрытый enum результатов адаптера (`SnmpGet(SnmpGetResponse)`, …). |
| **`UseCaseQuery`** | Закрытый enum валидированного запроса (часть `TaskSpec`). |
| **`TaskSpec`** | Готовая к сборке спека задачи: `meta + poll_config + query`. Из неё фабрика строит адаптер. |
| **Worker (актор)** | Владелец адаптера; исполняет расписание опроса, команды, метрики. Протокол-агностичен. |
| **`WorkerHandle`** | «Пульт» воркера: mailbox (`send`), `abort`, `is_finished`. |
| **Orchestrator** | Супервизор и рантайм: владеет репозиторием, хендлами воркеров, спеками и маппингом; принимает команды, слушает события, рассылает обновления. |
| **`OrchestratorHandle`** | Пульт оркестратора: `add_task`, `remove_task`, `start/stop/set_limit`, `get_snapshot`, `subscribe`. |
| **Task** | Логическая задача мониторинга = адаптер + расписание + история. |
| **Snapshot** | Снимок состояния задачи (результат + метрики + статус). |
| **Repository** | In-memory хранилище задач и их снапшотов. |
| **Application** | Тонкий клиент: конфиг-слой (валидация в `TaskSpec`) + обёртка над `OrchestratorHandle`. |
| **`PollResult`** | Итог воркера: `Initial / NoResponse / Fail / Success(Response<UseCaseOutput>)`. |
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
4. **Worker не знает протоколов.** Параметризован только `Pollable` и мостом
   `PollResult: From<Response<A::Output>>`.
5. **Закрытые enum'ы вместо `dyn` в ядре.** `UseCase`/`UseCaseOutput`/`PollResult` — закрытые
   множества; `dyn` допустим только на внешних точках расширения (например, history-sink).
6. **Оркестрация написана один раз.** Таймауты/ретраи/метрики/история — в core, не в адаптерах.
7. **Адаптер отвечает за интерпретацию.** Сырые байты → `BusinessValue` делает адаптер;
   воркер и UI получают нормализованные данные.
8. **Сборка адаптера — в фабрике (`UseCase::build`), а не в Orchestrator'е.** Orchestrator
   остаётся протокол-агностичным.

---

## 5. Действующие лица и сущности

### Компоненты (действующие лица)

| Компонент | Роль | Ключевые типы |
|---|---|---|
| **CLI** (`tctl`) | входная точка: парсит YAML, запускает, рендерит | `main.rs`, `AppBuilder`, formatters |
| **Application** | конфиг-слой + тонкий клиент: валидация конфига → `TaskSpec`, отправка команд | `Application`, `ApplicationId`, `ApplicationState` |
| **Orchestrator** | супервизор/рантайм: единственный владелец задач, воркеров и маппинга | `Orchestrator`, `OrchestratorHandle`, `OrchestratorCommand`, `OrchestratorEvent` |
| **PollWorker** | актор-исполнитель: расписание, ретраи, метрики, команды | `PollWorker<A>`, `WorkerHandle`, `WorkerCommand`, `WorkerEvent`, `WorkerState` |
| **UseCase (адаптер)** | «один опрос → типизированный результат» | `UseCase`, `UseCaseOutput`, `UseCaseQuery` |
| **TaskRepository** | in-memory хранилище состояния задач | `TaskRepository`, `TaskEntity`, `TaskSnapshot`, `TaskHistory` |

### Сущности (данные)

| Сущность | Что хранит |
|---|---|
| `TaskSpec` | `meta + poll_config + query` — всё для сборки адаптера и задачи |
| `TaskEntity` | задача: id, meta, snapshot, poll_config, history, created/updated |
| `TaskSnapshot` | `poll_result + metrics + poll_status` на момент времени |
| `Metrics` | счётчики попыток и латентность (total/success/errors, min/avg/max) |
| `PollResult` | результат опроса: `Success(Response<UseCaseOutput>)` или ошибка |
| `WorkerEvent` | сообщение воркера: `worker_id + state + metrics + poll_result` |

---

## 6. Слои и компоненты

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
│        │  валидация конфига            ▲   (workers + specs + mapping)      │
│        │                               │                                     │
│        │                    WorkerEvent (events канал)                        │
│        │                               │                                     │
│  ┌─────▼───────────────────────────────┴──────┐                             │
│  │ polling::worker                            │                             │
│  │   PollWorker<UseCase> (актор)              │                             │
│  │     mailbox (cmd_rx)  outbox (events_tx)   │                             │
│  │     WorkerHandle                           │                             │
│  └─────┬──────────────────────────────────────┘                             │
│        │ poll(&use_case) + метрики/ретраи/таймауты                           │
│  ┌─────▼──────────────────────────────┐                                      │
│  │ UseCase (адаптер, enum)            │  SnmpGet(SnmpReader), …              │
│  └─────┬──────────────────────────────┘                                      │
│  ┌─────▼──────────────────────────────┐                                      │
│  │ snmp (протокольный слой)           │  client, value, oid, profile,        │
│  │                                     │  registry, parsers                  │
│  └────────────────────────────────────┘                                      │
└────────────────────────────────────────────────────────────────────────────────┘
```

Правило зависимостей: слой зависит только от нижележащего; `polling` не знает про
`snmp`/`use-case`; `Orchestrator` оперирует `UseCase` (enum), не зная протокольных деталей.

---

## 7. Поток данных

### 7.1 Старт (создание задач)

```
YAML → AppBuilder → AppConfig
     → Application::new:
         для каждой TaskConfigDto → TaskSpec::try_from (валидация, без I/O)
         Orchestrator::new() → (orchestrator, handle); tokio::spawn(orchestrator.run())
         handle.add_task(spec) — на каждую спеку
     → Orchestrator.handle_command(AddTask):
         UseCase::build(spec.query, attempt)  → адаптер (async: коннект)
         TaskRepository.add_task(...)         → task_id
         PollWorker::spawn(worker_id, use_case, poll_config, events_tx) → WorkerHandle
         маппинг worker_id ↔ task_id; хранит spec (для будущего рестарта)
```

### 7.2 Запуск

```
Application::start()
  → handle.start_task(task_id) для каждой задачи
  → Orchestrator → WorkerHandle.send(WorkerCommand::Start)
  → воркер: Idle → Running, первый опрос сразу
```

### 7.3 Цикл опроса (горячий путь)

```
интервал сработал
  → PollWorker.handle_tick:
      poll(&attempt, &use_case) — с таймаутом/ретраями
      адаптер: устройство → сырые данные → UseCaseOutput
      Response { timestamp, attempts, errors, elapsed, payload }
      → PollResult::Success(...)  (или NoResponse/Fail)
      метрики обновляются
  → WorkerEvent { worker_id, state, metrics, poll_result }
  → events_tx (mpsc) → Orchestrator.handle_worker_event:
      worker_id → task_id (маппинг)
      TaskSnapshot → TaskRepository.update_task
      broadcast OrchestratorEvent::Update { snapshot, task_id }
  → UI: rx.recv() → format_repository(snapshot)
```

### 7.4 Горячее управление

```
SetLimit / Start / Stop:
  handle.set_limit / start_task / stop_task
  → OrchestratorCommand → Orchestrator → WorkerHandle.send(WorkerCommand)

RemoveTask:
  Orchestrator: WorkerHandle.abort() + чистка workers/worker_to_task/specs
              + TaskRepository.remove_task
  → возвращает удалённую TaskEntity (oneshot)
```

### 7.5 Супервизия (планируется)

```
периодически (health_interval):
  Orchestrator проходит по workers, ищет handle.is_finished()
  умерший воркер → rebuild из сохранённого spec → PollWorker::spawn → подмена handle
```

---

## 8. Взаимодействие между компонентами

Каналы связи (все — tokio-каналы):

| От → Кому | Канал | Что передаёт |
|---|---|---|
| Application → Orchestrator | `mpsc<OrchestratorCommand>` + `oneshot` (ответ) | AddTask/RemoveTask/Start/Stop/SetLimit/GetSnapshot/Subscribe |
| Orchestrator → Worker | `mpsc<WorkerCommand>` (через `WorkerHandle.send`) | Start/Stop/Resume/SetLimit |
| Worker → Orchestrator | `mpsc<WorkerEvent>` (общий, fan-in) | результаты опросов, состояние, метрики |
| Orchestrator → UI | `broadcast<OrchestratorEvent>` | снапшоты (обновления) |
| Orchestrator → history-sink (будущее) | `mpsc<HistoryRecord>` (надёжный, не broadcast) | записи истории |

Правила:

- **Команды — запрос-ответ** там, где нужен результат (`add_task` возвращает `TaskId`,
  `remove_task` — `TaskEntity`, `get_snapshot`/`subscribe` — через `oneshot`).
- **События воркеров — fan-in** в один `events`-канал оркестратора.
- **UI получает lossy `broadcast`** (отстающий подписчик теряет промежуточные кадры — для экрана ок).
- **История (если появится) — отдельный lossless `mpsc`** + writer-актор, чтобы медленный диск
  не тормозил горячий путь и данные не терялись.

---

## 9. Модель актора (Worker)

```rust
// Адаптер
#[async_trait]
pub trait Pollable: Send + Sync {
    type Output: Send;
    async fn poll(&self) -> Result<Self::Output, PollError>;
}

// Воркер — generic, создаёт свой mailbox сам
pub struct PollWorker<A: Pollable> { /* … */ }

impl<A: Pollable> PollWorker<A>
where PollResult: From<Response<A::Output>>
{
    // конструкция: создаёт mailbox, возвращает (Self, Sender<WorkerCommand>)
    pub fn new(id, adapter, poll_config, events_tx) -> (Self, mpsc::Sender<WorkerCommand>);

    // удобство: new + tokio::spawn(run) → WorkerHandle (требует A: 'static)
    pub fn spawn(id, adapter, poll_config, events_tx) -> WorkerHandle;

    #[tracing::instrument(name = "poll_worker", skip_all, fields(worker_id = %self.id))]
    pub async fn run(self) { /* select! { cmd | tick } */ }
}

// Пульт воркера
pub struct WorkerHandle {
    mailbox: mpsc::Sender<WorkerCommand>,
    join_handle: JoinHandle<()>,
}
impl WorkerHandle {
    pub async fn send(&self, cmd) -> Result<(), SendError>;
    pub fn abort(&self);
    pub fn is_finished(&self) -> bool;
}
```

Состояния и команды:

```rust
pub enum WorkerState { Idle, Running, Stopped, RatedLimit }
pub enum WorkerCommand { Start, Resume, Stop, SetLimit(u64) }
pub struct WorkerEvent { pub id, pub state, pub poll_config, pub metrics, pub poll_result }
```

Переходы: `Idle → Running` (`Start`), `Running → Stopped` (`Stop`),
`Running → RatedLimit` (исчерпан `limit`), `Stopped → Running` (`Resume`).

---

## 10. Точки расширения: рецепт добавления нового use-case

1. **Адаптер** — новый тип с `impl Pollable` (образец: `snmp/adapters/reader.rs`).
2. **`UseCase`** — новый вариант + arm в `poll()`.
3. **`UseCaseOutput`** — новый вариант.
4. **`UseCaseQuery`** — новый вариант + DTO в `config/`.
5. **`UseCase::build`** — arm, собирающий адаптер из query.
6. **Форматтер** — ветка в `match resp.payload` (`tools-cli/.../formatters/repository.rs`).

---

## 11. Контракты интерфейсов (сводка)

```rust
// расписание и попытка
pub struct AttemptConfig { pub timeout: Duration, pub retries: u8, pub retry_delay: Duration }
pub struct PollConfig  { pub interval: Duration, pub limit: u64, pub attempt: AttemptConfig }

// обёртка успешного ответа
pub struct Response<T> {
    pub timestamp: DateTime<Local>, pub attempts: u8,
    pub errors: Vec<PollErrorContext>, pub elapsed: Duration, pub payload: T,
}

// один опрос с ретраями (в core, один раз)
pub async fn poll<A: Pollable>(config: &AttemptConfig, adapter: &A)
    -> Result<Response<A::Output>, PollError>;

// итог воркера
pub enum PollResult {
    Initial,
    NoResponse(Vec<PollErrorContext>),
    Fail { message: String },
    Success(Response<UseCaseOutput>),
}

// метрики
pub struct Metrics { /* total/success/errors, current/avg/min/max latency */ }
```

---

## 12. Обработка ошибок

- **`PollError`** — сбой опроса (`NoResponse` / `Other`) → `PollResult::NoResponse/Fail`.
- **`SnmpError`** — протокол (таймаут, auth, OID, разбор).
- **`ParseError`** — интерпретация сырого значения в `BusinessValue`.
- **`BuildMonitorError`** — сборка адаптера в `UseCase::build`.
- **`OrchestratorError`** — команды/оркестрация (`Build`, `TaskRepository`, `ChannelClosed`).

Правило: ошибка интерпретации одного OID **не роняет** весь опрос — в `SnmpReader` она
превращается в `BusinessValue::Text("parse error")`, остальные OID доставляются.

---

## 13. Открытые вопросы

- **O-1. `PollResult`/`UseCase` — закрытый enum.** Ок для своих use-case'ов; `dyn` — только на
  внешних точках расширения (history-sink и т.п.).
- **O-2. Ответы команд актора.** Воркер пока fire-and-forget; `WorkerResponse` не используется.
  При необходимости — `oneshot` в `WorkerCommand`.
- **O-3. Супервизия.** Нужны health-check (`is_finished`/`JoinSet`), backoff и лимит рестартов,
  различение паники (`JoinError::is_panic`). `specs` уже хранятся.
- **O-4. Семантика `limit`.** Считает попытки (`total_attempts`), не успешные опросы/интервалы.
- **O-5. `Query` vs `UseCaseQuery`.** Дублирование на границе конфига; свести к одному набору.
- **O-6. Персистентность истории.** `TaskHistory` в памяти; нужен ли history-sink (файл/sqlite)
  через отдельный writer-актор + `mpsc`.
- **O-7. Автокорреляция между задачами.** Группировка уже есть; автоматическое сравнение
  снапшотов — опционально.
- **O-8. Границы «клиент vs адаптер».** Подключение/сериализация/декодирование — зафиксировать.

---

## 14. Дорожная карта

1. **Супервизия**: health-check + backoff/лимит рестартов + различение паники (O-3).
2. **History-sink** (O-6): `LogWriter` trait (`dyn`) + writer-актор + `mpsc`.
3. **Второй use-case** (http-read или icmp-ping) — проверить рецепт §10.
4. **snmp-set** — отдельный «командный» путь (не периодический опрос).
5. **Свести `Query`/`UseCaseQuery`/`TaskPollConfig`/`PollConfig`** (убрать дубли).
6. **Web-интерфейс** поверх `OrchestratorHandle` (subscribe уже есть).
7. **Параллельно**: самостоятельные инструменты `tools-core` (расчёты фаз/циклов/тактов,
   конвертеры, парсеры логов) — чистые функции, вне акторной модели.

---

## 15. Связанные файлы (карта)

| Компонент | Файл |
|---|---|
| Трейт адаптера | `crates/tools-core/src/polling/pollable.rs` |
| Опрос/расписание | `crates/tools-core/src/polling/{poll.rs, config.rs, metrics.rs}` |
| Воркер | `crates/tools-core/src/polling/worker/{worker.rs, env.rs}` |
| Итог воркера | `crates/tools-core/src/polling/poll_result.rs` |
| UseCase (адаптер/фабрика) | `crates/tools-core/src/monitor/application/use_case.rs` |
| Orchestrator | `crates/tools-core/src/monitor/application/orchestrator.rs` |
| Application (тонкий клиент) | `crates/tools-core/src/monitor/application/app.rs` |
| Конфиг/спеки | `crates/tools-core/src/monitor/application/config/{config.rs, snmp.rs, task_spec.rs, use_case_query.rs}` |
| Задачи/репозиторий | `crates/tools-core/src/monitor/{task.rs, task_repository.rs}` |
| SNMP-адаптер | `crates/tools-core/src/snmp/adapters/reader.rs` |
| Профили/реестр/парсеры | `crates/tools-core/src/snmp/{profiles.rs, registry/, parsers/}` |
| CLI | `crates/tools-cli/src/{main.rs, monitor/app.rs, monitor/queries/, monitor/formatters/}` |
