# Спецификация: архитектура сетевого опроса и монитора

> Статус: черновик (прототип).
> Область: подсистема «сетевые запросы + живой монитор» крейтов `tools-core` и `tctl`.
> Центральная идея: **независимые, переиспользуемые компоненты, собираемые под задачу**.

---

## 1. Цель и проблема

Транспортному инженеру постоянно приходится работать с **новым** оборудованием: появляется
новый дорожный контроллер, вендор добавляет новый OID, нужно быстро проверить значение,
затестировать новое устройство или проверить синхронизацию нескольких устройств.

Протоколы разные (SNMP сейчас; далее HTTP, Modbus, ICMP-ping и т.д.), а **оркестрация одинакова**:
периодичность, таймауты, ретраи, метрики, история, отображение.

Отсюда два требования к дизайну:

1. **Разделить** «что спросить и как понять ответ» (протокол) от «когда спросить и как управлять»
   (оркестрация).
2. **Сделать компоненты независимыми кирпичиками**, чтобы их можно было собирать в любые
   комбинации под конкретную задачу — без переписывания ядра.

---

## 2. Идея в одном абзаце

Есть набор независимых примитивов: **клиенты** (snmp/http/modbus/…), **адаптер-юскейс**
(реализует `Pollable`), **воркер-актор**, **task/snapshot/repository**. Любой, кто реализует
`Pollable` (и, при необходимости, `Updateble` для переподключения), передаётся воркеру:
воркер периодически вызывает `poll`, шлёт результат в канал и принимает команды.

`Application` — это **не обязательный путь, а дефолтная (эталонная) сборка**: она берёт конфиг,
создаёт клиенты → адаптеры → воркеры, регистрирует задачи и раздаёт обновления подписчикам.
Для быстрой проверки OID или нестандартного сценария можно собрать вручную только нужные
кирпичики (клиент + адаптер + воркер), минуя `Application`.

Это сочетание трёх паттернов:

1. **Ports & Adapters (hexagonal)** — core не зависит от транспорта; протоколы за адаптерами.
2. **Actor model** — воркер с mailbox, поведением и исходящими событиями.
3. **Enum-dispatch результатов** — закрытый `PollResult` как единый «язык» между воркером,
   репозиторием и UI.

---

## 3. Пример: «показать значения OID контроллера»

Пользователь хочет видеть текущие значения OID дорожного контроллера в реальном времени.

1. Пользователь описывает **конфигурацию**: хост, порт, community, профиль, список OID,
   интервал опроса.
2. Создаётся **use-case** (`SnmpReader`), реализующий `Pollable`: «один вызов `poll` → считал
   OID → декодировал → вернул `SnmpGetResponse`».
3. Создаётся **воркер** с этим use-case'ом: воркер по расписанию вызывает `poll`, отправляет
   результат (`WorkerEvent`/`PollResult`) в канал и принимает команды (`Start/Stop/…`).
4. Результат доходит до UI (через `Repository` + `broadcast`) и перерисовывается.

Тот же сценарий без `Application` (низкий уровень): `SnmpReadClient` + `SnmpReader` +
`PollWorker` + свой канал — для разовой проверки нового OID.

---

## 4. Глоссарий

| Термин | Смысл |
|---|---|
| **Клиент** | Тонкая обёртка над протоколом: `SnmpReadClient`, далее `HttpClient`, `ModbusClient`. Знает протокол, не знает расписание/задачи. |
| **Use-case / адаптер** | Тип запроса к устройству: `snmp-get` (реализован), далее `http-read`, `modbus-read`, `snmp-set`, `ping` и т.д. Реализует `Pollable`. |
| **`Pollable`** | Трейт адаптера: «выполни один опрос». |
| **`Updateble`** | Трейт адаптера: «пересоздай себя» (переподключение). |
| **Worker (актор)** | Владелец адаптера; исполняет расписание опроса, команды, метрики. Протокол-агностичен. |
| **Task** | Логическая задача мониторинга = адаптер + расписание + история. |
| **Snapshot** | Снимок состояния задачи на момент времени (результат + метрики + статус). |
| **Repository** | In-memory хранилище задач и снапшотов, точка подписки для UI. |
| **Application** | **Дефолтная сборка**: из конфига создаёт клиенты→адаптеры→воркеры, ведёт задачи, раздаёт обновления. |
| **`PollResult`** | Закрытый enum результатов всех use-case'ов — единый контракт с UI. |
| **SCN** | Site Code Number — ASCII-идентификатор контроллера, встраиваемый в OID (`.1.<len>.<bytes>`). |
| **Profile** | Вендор/протокол контроллера (Swarco, PotokS, PotokUg405, …); задаёт набор OID и правила SCN. |
| **Stage / фаза / такт** | Текущая фаза светофорного объекта. |

---

## 5. Принципы проектирования

1. **Независимость компонентов (главный).** Клиент, адаптер, воркер, task, repository —
   самодостаточные единицы. Любую можно использовать отдельно или собирать в цепочку.
   Новый протокол/устройство добавляется без правок ядра.
2. **`Application` — эталонная сборка, не обязательный путь.** Низкоуровневые сценарии
   (проверка OID, тест устройства) собираются напрямую из примитивов.
3. **Core не зависит от UI.** Никакого вывода на экран, терминала, форматирования в
   `tools-core`; форматтеры живут в `tctl` (`tools-cli/src/monitor/formatters/`).
4. **Worker не знает протоколов.** Он параметризован только `Pollable`/`Updateble` и мостом
   `PollResult: From<Response<A::Output>>`.
5. **Один use-case = один вариант в `PollResult` = один вариант в `Query`.**
   Расширение трогает фиксированный, небольшой набор файлов (рецепт в §9).
6. **Оркестрация написана один раз.** Таймауты/ретраи/метрики/история реализованы в core,
   не дублируются в каждом адаптере.
7. **Адаптер отвечает за интерпретацию.** Сырые байты → «бизнес-значение» (`BusinessValue`)
   делает адаптер через парсеры; воркер и UI получают уже нормализованные данные.

---

## 6. Композиция: строительные блоки и уровни сборки

```
БЛОКИ (снизу вверх, каждый независим):

  протокольный клиент     SnmpReadClient / (HttpClient, ModbusClient …)
        │  «знает протокол, не знает расписание»
        ▼
  адаптер (use-case)      SnmpReader: impl Pollable (+ Updateble)
        │  «один запрос → типизированный Output»
        ▼
  воркер (актор)          PollWorker<A: Pollable + Updateble>
        │  «расписание + команды + метрики + события в канал»
        ▼
  task / snapshot / repository   TaskEntity, TaskRepository, TasksRepoManager
        │  «состояние + история + подписка»
        ▼
  Application (дефолтная сборка)   из AppConfig собирает всё выше + broadcast в UI
```

**Уровни использования:**

| Уровень | Состав | Типичный сценарий |
|---|---|---|
| 1. Клиент | только `SnmpReadClient` | разовый `snmp-get` по OID |
| 2. Клиент + адаптер | `SnmpReadClient` + `SnmpReader`, вручную `poll()` | проверить декодирование нового OID |
| 3. Клиент + адаптер + воркер | + `PollWorker`, свой канал/команды | длительный опрос одного устройства без конфига |
| 4. `Application` | полная сборка из YAML | штатный монитор нескольких устройств |

Правило зависимостей: каждый блок зависит только от нижележащего; `polling` не знает о
`snmp`/`http`, `monitor` лишь склеивает адаптеры с воркерами по конфигу.

---

## 7. Слои и компоненты

```
                    ┌────────────────────────────────────────────┐
                    │                UI (tctl / будущий web)      │
                    │   подписка, форматирование, вывод           │
                    └──────────────────┬─────────────────────────┘
                                       │ broadcast: TasksRepoResponse
┌──────────────────────────────────────▼─────────────────────────────────────────┐
│                               tools-core                                       │
│  ┌───────────────────────────────────────────────────────────────────────────┐ │
│  │  monitor (application)                                                    │ │
│  │   Application ── создаёт ──► TaskRepository ──► TasksRepoManager          │ │
│  │        │  (владение задачами)        ▲               │ broadcast           │ │
│  └────────┼─────────────────────────────┼───────────────┼────────────────────┘ │
│           │ спавнит                     │ обновляет     │                       │
│  ┌────────▼───────────────┐   WorkerEvent(через manager)                       │
│  │ polling (worker)       │──────────────┘                                      │
│  │   PollWorker (актор)   │                                                     │
│  │     cmd_rx ── mailbox  │                                                     │
│  │     tx ────── outbox   │                                                     │
│  └────────┬───────────────┘                                                     │
│           │ poll(&adapter) + метрики/ретраи/таймауты                            │
│  ┌────────▼───────────────┐                                                     │
│  │ адаптер (use-case)     │  SnmpReader (сейчас), далее HttpReader, Modbus…     │
│  │   impl Pollable        │                                                     │
│  │   impl Updateble       │                                                     │
│  └────────┬───────────────┘                                                     │
│           │                                                                     │
│  ┌────────▼───────────────┐                                                     │
│  │ snmp (протокольный слой)│  client, value, oid, profile, registry, parsers   │
│  └────────────────────────┘                                                     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Модель актора (Worker)

### 8.1 Контракт (текущий код)

```rust
// Адаптер
#[async_trait]
pub trait Pollable: Send + Sync {
    type Output;
    async fn poll(&self) -> Result<Self::Output, PollError>;
}

#[async_trait]
pub trait Updateble: Send + Sync {
    type Instance;
    async fn update(self) -> Result<Self::Instance, UpdateError>;
}

// Актор: mailbox + outbox + поведение
pub struct PollWorker<A: Pollable + Updateble> {
    id: WorkerId,
    state: WorkerState,
    metrics: Metrics,
    tx: mpsc::Sender<WorkerEvent>,       // outbox
    cmd_rx: mpsc::Receiver<WorkerCommand>, // mailbox
    poll_config: PollConfig,
    adapter: A,
    interval_tick: tokio::time::Interval,
}

impl<A: Pollable + Updateble<Instance = A>> PollWorker<A>
where PollResult: From<Response<A::Output>>
{
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => self.handle_command(cmd).await,
                _   = self.interval_tick.tick() => self.handle_interval_tick().await,
            }
        }
    }
}
```

### 8.2 Состояния и команды

```rust
pub enum WorkerState { Idle, Running, Stopped, RatedLimit }

pub enum WorkerCommand { Start, Resume, Stop, SetLimit(u64) }

pub enum WorkerResponse { CurrentState(WorkerState) }   // объявлен, но НЕ используется

pub struct WorkerEvent {
    pub id: WorkerId,
    pub state: WorkerState,
    pub poll_config: PollConfig,
    pub metrics: Metrics,
    pub poll_result: PollResult,
}
```

Переходы: `Idle → Running` (`Start`), `Running → Stopped` (`Stop`),
`Running → RatedLimit` (при исчерпании `limit`), `Stopped/RatedLimit → Running` (`Resume`).

### 8.3 Зазоры относительно «настоящего» актора (целевое состояние)

| # | Текущее | Целевое (предлагается) |
|---|---|---|
| 1 | Команды «fire-and-forget», ответа нет; `WorkerResponse` мёртвый код | Каждая команда несёт `oneshot::Sender<WorkerResponse>` → актор отвечает ack/состоянием |
| 2 | `WorkerEvent` шлётся только из тика `handle_interval_tick`; `Stop` вообще не порождает событие (тик выходит по `if !is_running() return`) | Событие изменения состояния эмитится и из `handle_command`; UI видит смену `Running → Stopped` сразу |
| 3 | `Updateble::update()` не вызывается никем в runtime-пути | Определить политику переподключения: кто и когда зовёт `update()` (см. §13, вопрос O-3) |

---

## 9. Точки расширения: рецепт добавления нового use-case

Цель — чтобы добавление, например, `http-read` сводилось к фиксированному набору правок.

1. **Адаптер** — новый тип с `impl Pollable` (+ `impl Updateble` если нужно переподключение).
   Образец: `tools-core/src/snmp/adapters/reader.rs` (`SnmpReader`).
2. **`PollResult`** — новый вариант + мост:
   ```rust
   // tools-core/src/polling/poll_result.rs
   pub enum PollResult {
       Initial,
       NoResponse(Vec<PollErrorContext>),
       Fail { message: String },
       SnmpGet(Response<SnmpGetResponse>),
       HttpRead(Response<HttpReadResponse>),   // + новый
   }
   impl From<Response<HttpReadResponse>> for PollResult { /* … */ }
   ```
3. **Конфиг** — новый вариант `Query` и DTO:
   ```rust
   // tools-core/src/monitor/application/config/config.rs
   pub enum Query { SnmpGet(QuerySnmpGet), HttpRead(QueryHttpRead) }
   ```
4. **Сборка** — новая ветка в `Application::new` (создание адаптера + спавн воркера),
   `tools-core/src/monitor/application/app.rs`, `match &task.query { … }`.
5. **Десериализация** — `tools-cli/src/monitor/app.rs` (`TaskDto`, тэг `query_type`)
   и `tools-cli/src/monitor/queries/…`.
6. **Форматтер** — ветка в `match task_snapshot.poll_result()`,
   `tools-cli/src/monitor/formatters/repository.rs`.

Задел уже есть: `Protocol { Snmp, Http, Modbus }` объявлен (`monitor/task.rs`),
`TypeQuery` — только `SnmpGet` (расширить по мере появления).

---

## 10. Контракты интерфейсов (сводка)

```rust
// Попытка опроса: таймаут + ретраи
pub struct AttemptConfig { pub timeout: Duration, pub retries: u8, pub retry_delay: Duration }
// Расписание
pub struct PollConfig  { pub interval: Duration, pub limit: u64, pub attempt: AttemptConfig }

// Обёртка успешного ответа с диагностикой
pub struct Response<T> {
    pub timestamp: DateTime<Local>,
    pub attempts: u8,
    pub errors: Vec<PollErrorContext>,
    pub elapsed: Duration,
    pub payload: T,
}

// Один опрос с политикой ретраев (реализовано один раз в core)
pub async fn poll<A: Pollable>(config: &AttemptConfig, adapter: &A)
    -> Result<Response<A::Output>, PollError>;

// Метрики воркера
pub struct Metrics {
    pub total_attempts: u64, pub successful: u64, pub errors: u64,
    pub current_latency_ms: u64, pub avg_latency_ms: u64,
    pub min_latency_ms: u64, pub max_latency_ms: u64,
}
```

---

## 11. Сценарии использования (как блоки собираются под задачу)

| Сценарий | Сборка | Примечание |
|---|---|---|
| **Проверить новый OID** | уровень 1–2: клиент (+ адаптер) | разовый `poll`, без воркера и конфига |
| **Затестировать новое устройство/вендора** | уровень 3–4: адаптер + воркер (или `Application` с профилем) | новый `Profile` + парсеры в реестре |
| **Мониторинг парка устройств** | уровень 4: `Application` из YAML | штатный путь |
| **Сравнение нескольких устройств в одном мониторе** | уровень 4: один `Application`, несколько задач | штатный режим — см. пример ниже |
| **Нестандартный длительный опрос** | уровень 3: клиент+адаптер+воркер со своим каналом | минуя `Application` |

**Группировка задач** — это штатный режим `Application`: один `Application` владеет N задачами,
у каждой свой адаптер + воркер, свой интервал/лимит/профиль. UI выводит их вместе, что позволяет
инженеру визуально сопоставлять фазы разных контроллеров. Автоматическая корреляция значений
между задачами (например, «фазы разошлись») — потенциальное будущее расширение, а не базовое
требование (см. §13, O-7).

**Пример: два контроллера в одном мониторе (текущий CLI).**
Один `Application` собирает несколько задач; каждая — независимый воркер со своим устройством,
профилем, интервалом и лимитом (`attempt_config`/`deep_history` для краткости опущены):

```yaml
tasks:
  - name: "Фаза"                          # задача 1: Поток (UG-405), интервал 5с, без лимита
    interval_seconds: 5
    limit: 0
    query:
      query_type: snmpget
      profile: "potok_ug405"
      host: "127.0.0.1"
      port: 1162
      community: "public"
      oids:
        - { oid: ".1.3.6.1.4.1.13267.3.2.5.1.1.2.0", name: "Site ID" }
        - { oid: ".1.3.6.1.4.1.13267.3.2.5.1.1.3",  name: "Stage" }
        - { oid: ".1.3.6.1.4.1.13267.3.2.4.1.0",    name: "OperationMode" }
        - { oid: "stage",                           name: "From alias" }
  - name: "Фаза STCIP"                     # задача 2: Swarco, интервал 2с, лимит 5 опросов
    interval_seconds: 2
    limit: 5
    query:
      query_type: snmpget
      profile: "swarco"
      host: "127.0.0.1"
      port: 1161
      community: "public"
      oids:
        - { oid: ".1.3.6.1.4.1.1618.3.7.2.11.2", name: "Stage" }
        - { oid: "Фаза" }
```

Что иллюстрирует пример:

- **группировку**: два воркера с разными `PollConfig` (5с vs 2с) и разными адаптерами
  (`potok_ug405` vs `swarco`) в одном `Application`, выводятся бок о бок;
- **лимит**: `limit: 0` → бесконечно; `limit: 5` → после 5 опросов воркер переходит в `RateLimit`;
- **алиасы**: `oid: "stage"` и `oid: "Фаза"` резолвятся в числовой OID через реестр профиля;
- **SCN**: для UG-405 OID фазы дополняется SCN контроллера `CO101` → `…utcReplyGn.1.5.67.79.49.48.49`
  (тот же OID обслуживает разные контроллеры);
- **декодирование**: сырое OCTET STRING `08` → битовая маска → бизнес-значение «фаза 4».

---

## 12. Обработка ошибок

Три уровня ошибок (все через `thiserror`, корневой `Error` в `tools-core/src/error.rs`):

- **`PollError`** — сбой опроса: `NoResponse { errors }` (все ретраи неудачны) или
  `Other { message }` (ошибка адаптера). Конвертируется в `PollResult::NoResponse/Fail`.
- **`SnmpError`** — протокольный слой (таймаут, auth, неверный OID, разбор значения).
- **`ParseError`** — интерпретация сырого значения в `BusinessValue` (неверный тип/битмаска).

Правило: ошибка интерпретации одного OID **не роняет** весь опрос — в `SnmpReader` она
превращается в `BusinessValue::Text("parse error")`, остальные OID доставляются.

---

## 13. Открытые вопросы (нужно решение)

- **O-1. `PollResult` — закрытый enum или открытый механизм?**
  Закрытый enum даёт исчерпывающий match в UI, но требует правки core на каждый use-case.
  Альтернатива — запасной вариант `Other(…)` или `Box<dyn …>` для сторонних адаптеров.
  *Рекомендация:* пока закрытый enum (use-case'ы свои, в репозитории), при необходимости
  добавить `Other` позже.
- **O-2. Ответы команд актора.** Нужен ли строгий ack (`oneshot` в каждой команде) или
  достаточно push-событий? Влияет на API `Application::start/stop` и на будущий web.
- **O-3. Кто и когда зовёт `Updateble::update()`?** Варианты: (а) воркер сам при N подряд
  неудачных опросах, (б) внешняя команда `WorkerCommand::Reset`, (в) менеджер по событию.
- **O-4. Модель лимита.** Сейчас `limit` считает **попытки** (`total_attempts`), а не
  успешные опросы или интервалы. Уточнить семантику (`limit` = число опросов?).
- **O-5. Единый `Query` vs типизированный per-protocol.** Сейчас `Query` — enum с одним
  вариантом; при росте числа протоколов возможен вариант «общий конверт + payload».
- **O-6. Сохранение истории/снапшотов.** `TaskHistory` держит `VecDeque` в памяти; нужен ли
  сброс в файл/БД для длительных прогонов?
- **O-7. Автоматическое сравнение между задачами.** Группировка задач в одном `Application`
  уже есть (см. §11). Открытый вопрос — нужна ли *автоматическая* корреляция снапшотов
  нескольких задач (например, «фазы разошлись»), или достаточно визуального сопоставления.
- **O-8. Границы «клиента».** Что входит в протокольный клиент vs в адаптер: подключение,
  сериализация запроса, декодирование сырого значения? Зафиксировать контракт.

---

## 14. Дорожная карта (предложение)

1. **Закрыть акторные зазоры** (§8.3): ack команд + события изменения состояния + политика `Updateble`.
2. **Стабилизировать контракт `PollResult`/`Query`** — финальный вид точек расширения.
3. **Второй use-case** как образец расширения (рекомендую `http-read` или `icmp-ping` — минимальная семантика, не требует спец. оборудования).
4. **SNMP-write** (`snmp-set`) — уже есть заготовки `SnmpWriteClient`/`SnmpReadWriteClient`.
5. **(Опционально) автокорреляция между задачами** (O-7) — если визуального сопоставления окажется мало.
6. **Web-интерфейс** поверх того же `Application` (подписка уже есть — `broadcast`).
7. **Параллельно**: самостоятельные инструменты `tools-core` (расчёты фаз/циклов/тактов, конвертеры, парсеры логов) — как чистые функции/типы, вне акторной модели.

---

## 15. Связанные файлы (карта)

| Компонент | Файл |
|---|---|
| Трейты адаптера | `crates/tools-core/src/polling/pollable.rs`, `updateble.rs` |
| Один опрос с ретраями | `crates/tools-core/src/polling/poll.rs` |
| Актор | `crates/tools-core/src/polling/worker/worker.rs`, `env.rs` |
| Результаты use-case'ов | `crates/tools-core/src/polling/poll_result.rs` |
| SNMP-адаптер (образец) | `crates/tools-core/src/snmp/adapters/reader.rs` |
| Профили/реестр/парсеры | `crates/tools-core/src/snmp/{profiles.rs, registry/, parsers/}` |
| Оркестрация задач | `crates/tools-core/src/monitor/application/app.rs` |
| Хранилище | `crates/tools-core/src/monitor/task_repository.rs` |
| Конфиг CLI | `crates/tools-cli/src/monitor/app.rs`, `queries/` |
| Форматтеры | `crates/tools-cli/src/monitor/formatters/` |
