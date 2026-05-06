# FAQ - Поширені Запитання та Відповіді

## ❓ ЗАГАЛЬНІ ЗАПИТАННЯ

### Q1: Чим відрізняються три версії програм?

**db_queue_monitor.py (Основна):**
- Реалістичні параметри (генерування 3 запити/сек, обробка 0.6 сек/запит)
- Критичний поріг 50 запитів
- Менше імовірність перевищення черги в іншу динаміку
- **Рекомендується для:** Розуміння архітектури

**db_queue_monitor_demo.py (Демонстраційна):**
- Прискорені параметри (генерування 8 запити/сек, обробка 1.5 сек/запит)
- Критичний поріг лише 25 запитів
- **Гарантія:** Критична подія спрацює за 4-6 секунд
- **Рекомендується для:** Презентацій, демонстрацій

**db_queue_monitor_advanced.py (Розширена):**
- Реалізує додаткові завдання (timeout, multiple consumers, metrics)
- Логує запити в JSON файл
- Два обробники одночасно
- **Рекомендується для:** Авансованого аналізу, додаткових балів

---

### Q2: Як правильно запустити програму?

**Базовий варіант:**
```bash
python db_queue_monitor.py
```

**Демонстраційний варіант (рекомендується!):**
```bash
python db_queue_monitor_demo.py
```

**Розширений варіант:**
```bash
python db_queue_monitor_advanced.py
```

**Завершити програму:**
- Натисніть `Ctrl+C` - програма коректно завершиться
- Або дочекайтеся спрацювання критичної події (автоматичне завершення)

---

### Q3: Як довго чекати на критичну подію?

| Версія | Критичний поріг | Час до спрацювання |
|--------|---|---|
| **Основна** | 50 запитів | 30-60 сек (залежить від темпу) |
| **Демонстраційна** | 25 запитів | 4-6 сек (гарантовано!) |
| **Розширена** | 35 запитів | 10-15 сек |

**Порада:** Якщо чекаєте дуже довго на основній версії - можна натиснути Ctrl+C.

---

### Q4: Що означають символи в консолі?

| Символ | Значення |
|--------|----------|
| ✅ | Все в нормі |
| ⚠️ | Увага, близько до критичного |
| 🚨 | КРИТИЧНО! Виникла критична подія |
| 📊 | Статистика, метрики |
| 📨 | Запити генеруються |
| 📝 | Логування запитів |
| ⏱️ | Timeout сталася |
| 🚀 | Запуск системи |
| 🛑 | Зупинка системи |

---

## 🔍 ТЕХНІЧНІ ЗАПИТАННЯ

### Q5: Як працює asyncio.Queue?

```python
# Створення черги з обмеженням на 100 елементів
queue = asyncio.Queue(maxsize=100)

# Додавання без чекання (потенційна помилка!)
queue.put_nowait(item)

# Додавання з чеканням (якщо повна, чекає)
await queue.put(item)

# Отримання з чеканням (якщо пуста, чекає)
item = await queue.get()

# Позначення завершення обробки
queue.task_done()

# Поточний розмір
size = queue.qsize()
```

### Q6: Як працює asyncio.Event?

```python
# Створення eventos
event = asyncio.Event()

# Чекання на подію (тут заблокується до set())
await event.wait()

# Сигналізація про подію (розблоковує всі чекаючих)
event.set()

# Очищення события (для повторного використання)
event.clear()

# Перевірка статусу без чекання
is_set = event.is_set()
```

### Q7: Як реалізовується graceful shutdown?

```python
# 1. Встановлюємо флаг завершення
self.is_running = False

# 2. Скасовуємо всі таски
for task in tasks:
    task.cancel()

# 3. Чекаємо завершення всіх тасків
# (return_exceptions=True не кидає помилки)
await asyncio.gather(*tasks, return_exceptions=True)

# 4. Виводимо фінальну статистику та завершуємо
```

### Q8: Що таке timeout handling?

```python
try:
    # Чекаємо на результат максимум 3 секунди
    item = await asyncio.wait_for(queue.get(), timeout=3.0)
except asyncio.TimeoutError:
    # Якщо 3 сек нічого не прийшло - помилка
    print("Timeout! Дані не надійшли вчасно")
```

### Q9: Як мати двох споживачів з однієї черги?

```python
# Оба споживачи отримують з однієї черги
queue = asyncio.Queue()

async def consumer1():
    while True:
        item = await queue.get()
        # обробка item #1
        queue.task_done()

async def consumer2():
    while True:
        item = await queue.get()
        # обробка item #2
        queue.task_done()

# Запускаємо обидва одночасно
tasks = [
    asyncio.create_task(consumer1()),
    asyncio.create_task(consumer2()),
]
```

**Важливо:** Кожен `item` обробиться ОДНІЄЮ з обробників, не обома!

---

## 📊 АНАЛІЗ РЕЗУЛЬТАТІВ

### Q10: Як читати лог-файл queue_requests.log?

**Формат:** JSON (один запис на рядок)

**Перегляд у терміналі:**
```bash
# Перші 5 запитів
head -5 queue_requests.log

# Останні 5 запитів
tail -5 queue_requests.log

# Кількість запитів
wc -l queue_requests.log

# Пошук INSERT запитів
grep "INSERT" queue_requests.log

# Красивий вивід (якщо встановлено jq)
cat queue_requests.log | jq '.' | head -20
```

**Приклад запису:**
```json
{
  "request_id": 42,
  "query_type": "INSERT",
  "client_ip": "192.168.1.115",
  "timestamp": "2026-05-06T18:09:44.878774",
  "processed_at": "2026-05-06T18:09:44.878867"
}
```

### Q11: Як порахувати статистику з логів?

```bash
# Кількість SELECT запитів
grep -c "SELECT" queue_requests.log

# Кількість кожного типу
grep "query_type" queue_requests.log | grep -o '"[A-Z]*"' | sort | uniq -c

# Теплова карта IP-адрес (який клієнт найбільше запитує)
grep "client_ip" queue_requests.log | grep -o '192\.168\.1\.[0-9]*' | sort | uniq -c | sort -rn
```

---

## 🐛 РОЗВ'ЯЗАННЯ ПРОБЛЕМ

### Q12: Програма "зависла" на чеканні - це нормально?

**ТАК, це нормально!** Якщо запустили основну версію (`db_queue_monitor.py`):
- Програма чекає на спрацювання критичної события
- Це може зайняти від 30 до 60+ секунд
- Натисніть Ctrl+C для переривання

**Рекомендація:** Використовуйте `db_queue_monitor_demo.py` для швидкого результату.

### Q13: Criticial подія не спрацьовує ніколи

**Можливі причини:**
1. Обробка запитів занадто швидка (Consumer опережає Producer)
2. Criticial поріг занадто високий
3. Генерування запитів занадто повільне

**Розв'язання:**
```python
# Відредагуйте параметри в __init__:
system = DatabaseQueueMonitoringSystem(
    critical_queue_size=25,  # Зменшіть поріг
    max_queue_size=100
)

# Або запустіть demo версію
python db_queue_monitor_demo.py
```

### Q14: Помилка "asyncio.CancelledError"

**Це НОРМАЛЬНО!** Коли програма завершується:
- Усі таски скасовуються (cancel())
- Вони кидають `CancelledError`
- Ми ловимо цю помилку в `except asyncio.CancelledError`
- Це частина graceful shutdown

**Це НЕ помилка - це нормальне завершення!**

### Q15: Як змінити критичний поріг?

**Варіант 1 - Змінити константу в коді:**
```python
# У файлі db_queue_monitor.py, main() функції:
system = DatabaseQueueMonitoringSystem(
    critical_queue_size=35,  # Змініть це число
    max_queue_size=100
)
```

**Варіант 2 - Додати аргументи командного рядка:**
```python
import sys
critical_size = int(sys.argv[1]) if len(sys.argv) > 1 else 50
system = DatabaseQueueMonitoringSystem(critical_queue_size=critical_size)
```

---

## 📚 ДОДАТКОВІ РЕСУРСИ

### Офіційна документація

- **Python asyncio:** https://docs.python.org/3/library/asyncio.html
- **asyncio.Queue:** https://docs.python.org/3/library/asyncio-queue.html
- **asyncio.Event:** https://docs.python.org/3/library/asyncio-sync.html#asyncio.Event

### Бібліотеки для розширення

```bash
# Встановлення додаткових бібліотек
pip install aiofiles          # Асинхронна робота з файлами
pip install aiohttp           # Асинхронні HTTP запити
pip install asyncpg           # Асинхронний драйвер PostgreSQL
```

### Можливі розширення програми

1. **Асинхронна БД** - замість імітації
   ```python
   import asyncpg
   conn = await asyncpg.connect('postgresql://...')
   result = await conn.fetch('SELECT ...')
   ```

2. **REST API** - ekspozicyя метрик
   ```python
   from aiohttp import web
   # Додати web server для моніторингу
   ```

3. **Message Queue** - замість Queue
   ```python
   import aio_pika
   # Використання RabbitMQ замість asyncio.Queue
   ```

4. **Prometheus метрики** - для моніторингу
   ```python
   from prometheus_client import Counter, Gauge
   # Публікація метрик у Prometheus
   ```

---

## ✅ ПЕРЕД ЗДАЧЕЮ РОБОТИ

Переконайтесь, що у вас є:

- ✅ **REPORT.md** - повна документація
- ✅ **db_queue_monitor.py** - основна версія
- ✅ **db_queue_monitor_demo.py** - демонстраційна
- ✅ **db_queue_monitor_advanced.py** - розширена версія
- ✅ **queue_requests.log** - приклад логів
- ✅ **README.md** - інструкції
- ✅ **FAQ.md** - цей файл

**Усе готово до здачі!** 🎉

---

## 🎓 ВИСНОВКИ

### Чому асинхронність краще за синхронне програмування?

1. **Чекання без блокування**
   - Producer генерує, Consumer обробляє - одночасно
   - Без asyncio - один чекав би на іншого

2. **Ефективність ресурсів**
   - Один поток замість 3+ потоків OS
   - Менше памяти та контексту переключення

3. **Масштабованість**
   - Можна обробляти тисячи запитів одночасно
   - У нашому случае - 100+ запитів в черзі

4. **Простіша синхронізація**
   - Event, Queue - готові інструменти
   - Менше race conditions

5. **Явна логіка потоку**
   - `await` явно показує точки переключення
   - Легше отлаговувати та розуміти

---

**Питання або проблеми? - Переглядайте код та коментарі!** 💡
