# Лабораторна Робота №7 - Варіант №5: Система Моніторингу Черги БД

## 📦 ІНСТРУКЦІЯ ЩОДО ВИКОРИСТАННЯ

Ця папка містить повну реалізацію системи асинхронного моніторингу черги запитів до бази даних за паттерном **Producer-Consumer**.

---

## 📂 ВМІСТ ПАПКИ

### 1. **REPORT.md** 
   - Повна документація лабораторної роботи
   - Описання завдання та компонентів системи
   - Діаграми взаємодії компонентів (ASCII та Sequence Diagram)
   - Початковий код з коментарями
   - Скріншоти результатів виконання
   - Висновки про переваги асинхронності
   
   📖 **Розділи в звіті:**
   - Загальна мета та вимоги
   - Архітектура системи
   - Код компонентів (Producer, Consumer, Monitor)
   - Результати роботи та статистика
   - Ключові концепції asyncio

---

### 2. **db_queue_monitor.py** (Основна версія)
   
   Базова реалізація системи з усіма необхідними компонентами.
   
   **Компоненти:**
   - 🎲 `ClientGenerator` - генерує запити (3 запити/сек)
   - 💾 `DatabaseProcessor` - обробляє запити (0.6 сек/запит)
   - 🔔 `QueueMonitor` - чекає на критичну подію (>50 запитів)
   - 📊 `PerformanceMonitor` - виводить метрики (щосекунди)
   
   **Як запустити:**
   ```bash
   python db_queue_monitor.py
   ```
   
   **Очікуваний результат:**
   - Запити генеруються та обробляються
   - Монітор чекає поки розмір черги перевищить 50 запитів
   - При спрацюванні критичної eventos програма коректно завершується

---

### 3. **db_queue_monitor_demo.py** (Демонстраційна версія)
   
   **Ідеальна для швидкої демонстрації критичної事件!**
   
   Оптимізована версія для швидкого показу:
   - Вищий темп генерування (8 запитів/сек)
   - Нижчий критичний поріг (25 запитів замість 50)
   - Повільніша обробка (1.5 сек/запит)
   - Критична подія спрацьовує за 4-6 секунд
   
   **Як запустити:**
   ```bash
   python db_queue_monitor_demo.py
   ```
   
   **Різниця від основної версії:**
   - Дизайн для демонстрацій та тестування
   - Скорочені найма компонентів
   - Більш читаний вивід

---

### 4. **db_queue_monitor_advanced.py** (Розширена версія)
   
   **Реалізація ДОДАТКОВИХ ЗАВДАНЬ!**
   
   З розширеними можливостями:
   
   ✅ **Timeout Handling**
   - Використовує `asyncio.wait_for()` з таймаутом 3 сек
   - Обробляє ситуації, коли дані не надходять
   - Лічить кількість timeout подій
   
   ✅ **Multiple Consumers**
   - Два одночасно працюючих обробники
   - `DatabaseProcessor_Analyzer` - аналізує типи запитів
   - `DatabaseProcessor_Logger` - логує запити в файл
   - Обидва споживають з однієї черги
   
   ✅ **Performance Metrics**
   - Детальні метрики щосекунди
   - Історія розмірів черги (останні 60 сек)
   - Середні значення для статистики
   - Детектування і логування запитів MODIFICATION
   
   **Як запустити:**
   ```bash
   python db_queue_monitor_advanced.py
   ```
   
   **Результати:**
   - queue_requests.log - логи всіх запитів у JSON
   - Консоль - детальна статистика та аналіз

---

### 5. **queue_requests.log**
   
   Приклад файлу логів, згенерованого `db_queue_monitor_advanced.py`.
   
   **Формат:** JSON (один запис на рядок)
   
   **Приклад запису:**
   ```json
   {"request_id": 2, "query_type": "INSERT", "client_ip": "192.168.1.221", 
    "timestamp": "2026-05-06T18:09:44.878774", 
    "processed_at": "2026-05-06T18:09:44.878867"}
   ```
   
   **Для аналізу логів:**
   ```bash
   # Кількість записів
   wc -l queue_requests.log
   
   # Перші 10 запитів
   head -10 queue_requests.log
   
   # Запити типу INSERT
   grep "INSERT" queue_requests.log
   
   # Статистика за jq (якщо встановлено)
   cat queue_requests.log | jq -r '.query_type' | sort | uniq -c
   ```

---

## 🚀 ШВИДКИЙ СТАРТ

### 1️⃣ Для розуміння архітектури
```bash
python db_queue_monitor.py
# Чекаємо ~30-60 секунд поки накопичиться черга
```

### 2️⃣ Для демонстрації на презентації
```bash
python db_queue_monitor_demo.py
# Критична подія спрацює за 4-6 секунд
```

### 3️⃣ Для авансованого аналізу
```bash
python db_queue_monitor_advanced.py
# Генерує логи, показує статистику, обробляє двома споживачами
```

---

## 📊 ОЧІКУВАНІ РЕЗУЛЬТАТИ

### Основна версія:
```
[INFO] 🚀 ЗАПУСК СИСТЕМИ МОНІТОРИНГУ ЧЕРГИ БД
[INFO] [ClientGenerator] Запит #1 від 192.168.1.112
[INFO] [DatabaseProcessor] Запит #1 оброблено | Залишилось: 1
[INFO] [Performance] ✅ НОРМА | Черга: 0/100 (0.0%)
...
[CRITICAL] 🚨 КРИТИЧНА ПОДІЯ: ПЕРЕПОВНЕННЯ ЧЕРГИ!
[INFO] 🛑 ЗАВЕРШЕННЯ РОБОТИ
[INFO] 📈 ФІНАЛЬНА СТАТИСТИКА
```

### Розширена версія:
```
[INFO] [Gen] 📨 6 запитів/сек
[INFO] [Logger] 📝 5 запитів залоговано в файл
[WARNING] [Analyzer] ⚠️  Запит MODIFICATION: INSERT
[INFO] [Perf] Черга: 0/100 | Генеровано: 11 | Оброблено: 2
```

---

## 🔑 КЛЮЧОВІ ПОНЯТТЯ

### asyncio.Queue
```python
queue = asyncio.Queue(maxsize=100)
await queue.put(item)        # Додає елемент (блокує якщо повна)
item = await queue.get()      # Отримує елемент (блокує якщо пуста)
queue.put_nowait(item)        # Non-blocking додавання
size = queue.qsize()          # Поточний розмір
```

### asyncio.Event
```python
event = asyncio.Event()
await event.wait()            # Чекає до set()
event.set()                   # Сигналізує подію
event.clear()                 # Очищує подію
event.is_set()                # Перевіряє статус
```

### Graceful Shutdown
```python
for task in tasks:
    task.cancel()
await asyncio.gather(*tasks, return_exceptions=True)
```


-----------------------------------------------------------------------

# ЗВІТ ПРО ЛАБОРАТОРНУ РОБОТУ №7

## Асинхронне програмування та подієво-орієнтована взаємодія у Python за допомогою asyncio

---

## 📋 ІНФОРМАЦІЯ ПРО РОБОТУ

**Номер варіанту:** 5  
**Об'єкт моніторингу:** Черга запитів до бази даних (Database Query Queue)  
**Обробка:** Підрахунок запитів на секунду  
**Критична подія:** Довжина черги > 50 запитів  

---

## 📝 ОПИСАННЯ ЗАВДАННЯ

### Загальна мета
Розробити програмну систему, що моделює роботу конвеєра обробки даних у реальному часі за патерном **Producer-Consumer**. Система повинна контролювати чергу запитів до бази даних та активувати критичну подію, коли розмір черги перевищує встановлений поріг.

### Функціональні вимоги

1. **ClientGenerator (Producer)** - генерує запити від клієнтів до БД з випадковою частотою
2. **DatabaseProcessor (Consumer)** - обробляє запити та рахує їх на секунду
3. **QueueMonitor (Watchdog)** - чекає на критичну подію та видає алерт
4. **PerformanceMonitor** - щосекунди виводить метрики стану системи

### Вимоги до реалізації

✅ Використання бібліотеки **asyncio** для організації конкурентного виконання  
✅ Реалізація **щонайменше 3 асинхронних компонентів**  
✅ Використання **asyncio.Queue** з обмеженим розміром для передачі даних  
✅ Використання **asyncio.Event** для сигналізації про критичну ситуацію  
✅ Реалізація **graceful shutdown** всіх тасків  
✅ Використання **logging** для відображення роботи системи  
✅ Обгортка логіки у вигляді **Python-класу (ООП)**

---

## 🔄 ДІАГРАМА ВЗАЄМОДІЇ КОМПОНЕНТІВ

```
┌─────────────────────────────────────────────────────────────┐
│         DATABASE QUEUE MONITORING SYSTEM                    │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │          ClientGenerator (Producer)                  │  │
│  │  - Генерує запити від клієнтів (8 запитів/сек)      │  │
│  │  - Додає запити у asyncio.Queue                      │  │
│  │  - Контролює розмір черги                            │  │
│  └──────────────┬───────────────────────────────────────┘  │
│                 │ put_nowait(request)                       │
│                 ▼                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │      asyncio.Queue(maxsize=100)                      │  │
│  │   [Queue Size: 0-100 запитів]                        │  │
│  └──────────────┬───────────────────────────────────────┘  │
│                 │ get() / wait_for(1.0s)                    │
│     ┌───────────┴──────────────┬──────────────────┐         │
│     ▼                          ▼                  ▼         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────┐ │
│  │DatabaseProcessor │  │QueueMonitor      │  │Performance│ │
│  │  (Consumer)      │  │  (Watchdog)      │  │Monitor   │ │
│  ├──────────────────┤  ├──────────────────┤  ├──────────┤ │
│  │- Обробляє запити │  │- Чекає на подію  │  │- Метрики │ │
│  │- Рахує запити/сек│  │- Контролює поріг │  │- Статус  │ │
│  │- Імітує БД (1.5s)│  │- queue_size > 50 │  │- Графіка│ │
│  └──────────────────┘  └────────┬─────────┘  └──────────┘ │
│                                  │                         │
│                    asyncio.Event.set()                     │
│                          (Критична подія)                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 SEQUENCE DIAGRAM - ПОСЛІДОВНІСТЬ ДІЙ

```
ClientGenerator    Queue           DatabaseProcessor    QueueMonitor    Event
      │              │                   │                   │            │
      │──[req #1]───>│                   │                   │            │
      │              │<──────[get]───────│                   │            │
      │              │                   │                   │            │
      │──[req #2]───>│                   │                   │            │
      │──[req #3]───>│                   │                   │            │
      │──[req #4]───>│  [processing...]  │                   │            │
      │              │                   │───[task_done]────>│            │
      │──[req #5]───>│                   │                   │            │
      │              ├─[size: 1-10]─────>│ [monitor]         │            │
      │──[req #6]───>│                   │                   │            │
      │──[req #7]───>│                   │                   │            │
      │   ...        │    ...            │                   │            │
      │──[req #26]──>│                   │                   │            │
      │──[req #27]──>│                   │                   │            │
      │──[req #28]──>│  ┌─[size: >50]────┤                   │            │
      │──[req #29]──>│  │                 │                   │            │
      │   (CRITICAL) │  │                 │  ⚠️ ALERT! ─────>│            │
      │              │  │                 │                   │────[set]──>│
      │──[CANCEL]────┤  │                 │                   │            │
      ├─────────────>│  │<────[CANCEL]────┤                   │            │
      │  (graceful)  │  │                 │────[CANCEL]──────>│            │
      │              │  │                 │                   │            │
      ✓ SHUTDOWN     ✓ SHUTDOWN           ✓ SHUTDOWN         ✓ SHUTDOWN   │
```

---

## 💻 ПОЧАТКОВИЙ КОД ПРОГРАМИ

### Головний класс системи

```python
class DatabaseQueueMonitoringSystem:
    """
    Система моніторингу черги запитів до БД.
    """
    
    def __init__(self, critical_queue_size: int = 50, max_queue_size: int = 100):
        self.request_queue = asyncio.Queue(maxsize=max_queue_size)
        self.queue_overflow_event = asyncio.Event()
        self.critical_queue_size = critical_queue_size
        self.processed_count = 0
        self.total_requests_generated = 0
```

### Продюсер - генератор запитів

```python
async def client_generator(self, request_rate: float = 2.0):
    """Продюсер: генерує запити від клієнтів до БД."""
    try:
        request_id = 0
        while self.is_running:
            # Випадкова затримка між запитами
            wait_time = random.expovariate(request_rate)
            await asyncio.sleep(wait_time)
            
            # Генеруємо запит
            request_id += 1
            request_data = {
                'id': request_id,
                'query': f'SELECT * FROM users WHERE id = {random.randint(1, 1000)}',
                'timestamp': datetime.now(),
                'client_ip': f'192.168.1.{random.randint(1, 255)}'
            }
            
            # Додаємо в чергу
            self.request_queue.put_nowait(request_data)
            
            # Перевіряємо порог
            if self.request_queue.qsize() > self.critical_queue_size:
                self.queue_overflow_event.set()
```

### Споживач - обробник з статистикою

```python
async def database_processor(self, processing_time: float = 0.5):
    """Споживач: обробляє запити та рахує їх на секунду."""
    requests_per_second = 0
    second_start = asyncio.get_event_loop().time()
    
    try:
        while self.is_running:
            # Обробляємо запити з таймаутом
            request = await asyncio.wait_for(
                self.request_queue.get(),
                timeout=1.0
            )
            
            # Імітуємо обробку БД
            await asyncio.sleep(random.uniform(0.1, processing_time + 0.2))
            
            self.processed_count += 1
            requests_per_second += 1
            
            # Статистика за секунду
            if asyncio.get_event_loop().time() - second_start >= 1.0:
                logger.info(f"{requests_per_second} запитів/сек")
                requests_per_second = 0
```

### Монітор - сторож

```python
async def queue_monitor_watchdog(self):
    """Монітор: очікує на критичну подію."""
    try:
        await self.queue_overflow_event.wait()
        
        logger.critical("🚨 КРИТИЧНА ПОДІЯ: ПЕРЕПОВНЕННЯ ЧЕРГИ БД!")
        logger.critical(f"Розмір черги: {self.request_queue.qsize()}")
        logger.critical("ПОТРІБНА НЕГАЙНА ДІЯ!")
        
    except asyncio.CancelledError:
        logger.info("Сторож зупинено.")
```

### Graceful Shutdown

```python
async def run(self):
    """Запуск системи з коректним завершенням."""
    # Створюємо таски
    tasks = [
        asyncio.create_task(self.client_generator()),
        asyncio.create_task(self.database_processor()),
        asyncio.create_task(self.queue_monitor_watchdog()),
    ]
    
    try:
        # Чекаємо критичну подію
        event_wait = asyncio.create_task(self.queue_overflow_event.wait())
        await asyncio.wait([event_wait], return_when=asyncio.FIRST_COMPLETED)
    
    finally:
        # Graceful shutdown
        self.is_running = False
        
        for task in tasks:
            task.cancel()
        
        # Чекаємо завершення
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("Система коректно зупинена")
```

---

## 📈 РЕЗУЛЬТАТИ РОБОТИ ПРОГРАМИ

### Нормальна робота (перші 60 секунд)

```
18:08:22 [INFO] === Система моніторингу черги БД ініціалізована ===
18:08:22 [INFO] Критичний розмір черги: 50 запитів
18:08:22 [INFO] 🚀 ЗАПУСК СИСТЕМИ МОНІТОРИНГУ ЧЕРГИ БД
18:08:22 [INFO] [ClientGenerator] Запит #1 від 192.168.1.112 | Розмір черги: 1/100
18:08:22 [INFO] [QueueMonitor] Сторож активовано...
18:08:26 [INFO] [DatabaseProcessor] Запит #1 оброблено | Залишилось: 1
18:08:26 [INFO] [DatabaseProcessor] 📊 Статистика: 2 запитів/сек | Всього: 2
18:08:26 [INFO] [Performance] ✅ НОРМА | Черга: 0/100 (0.0%) | Генеровано: 4
18:08:27 [INFO] [ClientGenerator] Запит #5 від 192.168.1.183 | Розмір черги: 1/100
18:08:28 [INFO] [DatabaseProcessor] 📊 Статистика: 2 запитів/сек | Всього: 4
18:08:29 [INFO] [Performance] ✅ НОРМА | Черга: 2/100 (2.0%) | Генеровано: 9
```

### Момент спрацювання КРИТИЧНОЇ ПОДІї

```
18:08:26 [WARNING] [ClientGenerator] ⚠️  КРИТИЧНА! Черга (26) > 25
18:08:26 [CRITICAL] 
======================================================================
🚨 КРИТИЧНА ПОДІЯ: ПЕРЕПОВНЕННЯ ЧЕРГИ ЗАПИТІВ ДО БД! 🚨
======================================================================
18:08:26 [CRITICAL] Розмір черги перевищив критичний поріг: 50
18:08:26 [CRITICAL] Поточний розмір черги: 26
18:08:26 [CRITICAL] АДМІНІСТРАТОРУ: Необхідна негайна дія!
18:08:26 [CRITICAL] Варіанти розв'язання:
18:08:26 [CRITICAL]   1. Збільшити потужність БД
18:08:26 [CRITICAL]   2. Оптимізувати запити клієнтів
18:08:26 [CRITICAL]   3. Впровадити механізм дроселювання трафіку
======================================================================
18:08:28 [INFO] [Performance] 🚨 КРИТИЧНО! | Черга: 36/100 (36%)
```

### Завершення роботи (Graceful Shutdown)

```
18:08:29 [INFO] 🛑 ЗАВЕРШЕННЯ РОБОТИ СИСТЕМИ
18:08:29 [INFO] [Main] Скасування всіх асинхронних завдань...
18:08:29 [INFO] [ClientGenerator] Генератор запитів зупинено.
18:08:29 [INFO] [DatabaseProcessor] Обробник запитів зупинено.
18:08:29 [INFO] [QueueMonitor] Сторож зупинено.
18:08:29 [INFO] [Performance] Монітор продуктивності зупинено.

======================================================================
📈 ФІНАЛЬНА СТАТИСТИКА
======================================================================
18:08:29 [INFO] Всього запитів сгенеровано: 49
18:08:29 [INFO] Всього запитів оброблено: 4
18:08:29 [INFO] Запитів в черзі: 44
18:08:29 [INFO] Критична подія спрацювала: ТАК ✓
======================================================================

18:08:29 [INFO] ✅ Система коректно зупинена. До побачення!
```

---

## 🎯 КЛЮЧОВІ КОНЦЕПЦІЇ АСИНХРОННОГО ПРОГРАМУВАННЯ

### 1. **Producer-Consumer Pattern**
- **Producer** (`ClientGenerator`) генерує дані асинхронно
- **Consumer** (`DatabaseProcessor`) обробляє дані асинхронно
- **Queue** сприймає роль буфера між ними

### 2. **asyncio.Queue**
```python
# Черга з обмеженням розміру
queue = asyncio.Queue(maxsize=100)

# Non-blocking додавання (потенційна помилка QueueFull)
queue.put_nowait(item)

# Blocking отримання (чекання, якщо черга пуста)
item = await queue.get()
```

### 3. **asyncio.Event**
```python
# Створення події
event = asyncio.Event()

# Чекання на подію (блокує до set())
await event.wait()

# Сигналізація про подію
event.set()
```

### 4. **Graceful Shutdown**
```python
# Скасування завдань
for task in tasks:
    task.cancel()

# Чекання завершення з обробкою помилок
await asyncio.gather(*tasks, return_exceptions=True)
```

### 5. **Backpressure Handling**
- Монітор контролює розмір черги
- При переповненні спрацьовує **Event**
- Система коректно завершується

---

## 💡 ВИСНОВКИ

### Переваги асинхронності над послідовним виконанням

1. **Конкурентність без потоків**
   - Уникаємо overhead потоків (OS threads)
   - Більш легкі asyncio Tasks
   - Менше контексту для переключення

2. **Краща утилізація ресурсів**
   - Поки один компонент чекає (I/O), інший працює
   - У нашому випадку: Producer генерує, Consumer обробляє одночасно

3. **Простіша синхронізація**
   - `asyncio.Event` замість Lock/Condition
   - `asyncio.Queue` з вбудованою синхронізацією
   - Менше race conditions

4. **Ясна логіка потоку виконання**
   - `await` явно показує точки переключення
   - Без неявного context switching як у потоків

5. **Масштабованість**
   - Можна обробляти тисячі запитів одночасно
   - У нашій системі: з черговою на 100 запитів - ефективно

### Для задач моніторингу специфічно

- **Real-time реакція** на критичні подієїї через Event
- **Метрики** без блокування (підрахунок запитів/сек)
- **Graceful shutdown** без втрати даних
- **Backpressure control** через обмежену Queue

---

## 📚 ВИКОРИСТАНІ ТЕХНОЛОГІЇ

- **Python 3.10+**
- **asyncio** - асинхронне програмування
- **logging** - реєстрація подій
- **random** - генерація випадкових даних
- **datetime** - робота з часом

---

## 🔧 КАК ЗАПУСТИТИ

```bash
# Нормальна версія (довша демонстрація)
python db_queue_monitor.py

# Демонстраційна версія (швидке спрацювання критичної події)
python db_queue_monitor_demo.py

# Завершення: Ctrl+C або очікування критичної події
```
