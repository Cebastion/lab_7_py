"""
Лабораторна робота №7. Варіант №5: Моніторинг черги запитів до БД

Завдання:
- Об'єкт моніторингу: Черга запитів до БД
- Обробка: Підрахунок запитів на секунду
- Критична подія: Довжина черги > 50 запитів

Компоненти системи:
1. ClientGenerator (Producer) - генерує запити від клієнтів
2. DatabaseProcessor (Consumer) - обробляє запити та рахує їх на секунду
3. QueueMonitor (Watchdog) - очікує критичної ситуації та видає алерт
"""

import asyncio
import random
import logging
from datetime import datetime
from typing import Optional

# ============================================================================
# НАЛАШТУВАННЯ ЛОГУВАННЯ
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# ГОЛОВНИЙ КЛАС СИСТЕМИ МОНІТОРИНГУ
# ============================================================================

class DatabaseQueueMonitoringSystem:
    """
    Система моніторингу черги запитів до бази даних.
    
    Атрибути:
        request_queue (asyncio.Queue): Черга запитів до БД (макс. 100 елементів)
        queue_overflow_event (asyncio.Event): Подія для сигналізації про переповнення черги
        critical_queue_size (int): Критичний розмір черги (50 запитів)
        processed_count (int): Лічильник оброблених запитів
        is_running (bool): Статус роботи системи
    """
    
    def __init__(self, critical_queue_size: int = 50, max_queue_size: int = 100):
        """
        Ініціалізація системи.
        
        Args:
            critical_queue_size: Порогове значення довжини черги для спрацювання EVENT
            max_queue_size: Максимальний розмір черги в памяті
        """
        self.request_queue = asyncio.Queue(maxsize=max_queue_size)
        self.queue_overflow_event = asyncio.Event()
        self.critical_queue_size = critical_queue_size
        self.max_queue_size = max_queue_size
        
        # Лічильники для статистики
        self.processed_count = 0
        self.total_requests_generated = 0
        self.is_running = True
        
        logger.info("=== Система моніторингу черги БД ініціалізована ===")
        logger.info(f"Критичний розмір черги: {critical_queue_size} запитів")
        logger.info(f"Максимальний розмір черги: {max_queue_size} запитів")

    # ========================================================================
    # PRODUCER: ГЕНЕРАТОР ЗАПИТІВ
    # ========================================================================

    async def client_generator(self, request_rate: float = 2.0):
        """
        Продюсер: генерує запити від клієнтів до БД.
        
        Симулює клієнтів, які відправляють запити з випадковими інтервалами.
        
        Args:
            request_rate: середня кількість запитів на секунду (за Пуассоновим розподілом)
        """
        try:
            request_id = 0
            while self.is_running:
                # Генеруємо випадкову затримку між запитами
                # (використовуємо експоненціальний розподіл)
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
                
                try:
                    # Додаємо запит у чергу без чекання (non-blocking для демонстрації)
                    self.request_queue.put_nowait(request_data)
                    self.total_requests_generated += 1
                    
                    queue_size = self.request_queue.qsize()
                    logger.info(
                        f"[ClientGenerator] Запит #{request_id} від {request_data['client_ip']} | "
                        f"Розмір черги: {queue_size}/{self.max_queue_size}"
                    )
                    
                    # Перевіряємо, чи перевищена критична довжина черги
                    if queue_size > self.critical_queue_size and not self.queue_overflow_event.is_set():
                        logger.warning(
                            f"[ClientGenerator] ⚠️  КРИТИЧНА СИТУАЦІЯ! "
                            f"Розмір черги ({queue_size}) > порогу ({self.critical_queue_size})"
                        )
                        self.queue_overflow_event.set()
                
                except asyncio.QueueFull:
                    logger.error("[ClientGenerator] Черга переповнена! Запит втрачено.")
                    
        except asyncio.CancelledError:
            logger.info("[ClientGenerator] Генератор запитів зупинено.")

    # ========================================================================
    # CONSUMER: ОБРОБНИК ЗАПИТІВ З ПІДРАХУНКОМ
    # ========================================================================

    async def database_processor(self, processing_time: float = 0.5):
        """
        Споживач: обробляє запити та рахує їх на секунду.
        
        Симулює обробку запиту БД та збирає статистику.
        
        Args:
            processing_time: середній час обробки одного запиту в секундах
        """
        requests_per_second = 0
        second_start = asyncio.get_event_loop().time()
        
        try:
            while self.is_running:
                try:
                    # Очікуємо запит з черги (з таймаутом)
                    request = await asyncio.wait_for(
                        self.request_queue.get(),
                        timeout=1.0
                    )
                    
                    # Імітуємо обробку запиту БД
                    processing_delay = random.uniform(
                        max(0.1, processing_time - 0.2),
                        processing_time + 0.2
                    )
                    await asyncio.sleep(processing_delay)
                    
                    self.processed_count += 1
                    requests_per_second += 1
                    queue_size = self.request_queue.qsize()
                    
                    logger.info(
                        f"[DatabaseProcessor] Запит #{request['id']} оброблено "
                        f"(від {request['client_ip']}) | "
                        f"Залишилось у черзі: {queue_size}"
                    )
                    
                    self.request_queue.task_done()
                    
                    # Кожну секунду виводимо статистику
                    current_time = asyncio.get_event_loop().time()
                    if current_time - second_start >= 1.0:
                        logger.info(
                            f"[DatabaseProcessor] 📊 Статистика: "
                            f"{requests_per_second} запитів/сек | "
                            f"Всього оброблено: {self.processed_count}"
                        )
                        requests_per_second = 0
                        second_start = current_time
                
                except asyncio.TimeoutError:
                    # Якщо 1 сек нічого не надійшло, виводимо статистику з нулем
                    current_time = asyncio.get_event_loop().time()
                    if current_time - second_start >= 1.0:
                        logger.info(
                            f"[DatabaseProcessor] 📊 Статистика: "
                            f"{requests_per_second} запитів/сек | "
                            f"Всього оброблено: {self.processed_count}"
                        )
                        requests_per_second = 0
                        second_start = current_time
                        
        except asyncio.CancelledError:
            logger.info("[DatabaseProcessor] Обробник запитів зупинено.")

    # ========================================================================
    # MONITOR: СТОРОЖ ДЛЯ ВІДСТЕЖЕННЯ КРИТИЧНОЇ СИТУАЦІЇ
    # ========================================================================

    async def queue_monitor_watchdog(self):
        """
        Монітор: очікує на подію переповнення черги та видає критичне попередження.
        
        Чекає поки розмір черги перевищить критичне значення, 
        а потім видає алерт та інформує адміністратора.
        """
        try:
            logger.info("[QueueMonitor] Сторож активовано. Очікування на критичну подію...")
            
            # Чекаємо на подію переповнення
            await self.queue_overflow_event.wait()
            
            # Видаємо критичне попередження
            logger.critical(
                "\n" + "="*70 + "\n"
                "🚨 КРИТИЧНА ПОДІЯ: ПЕРЕПОВНЕННЯ ЧЕРГИ ЗАПИТІВ ДО БД! 🚨\n"
                "="*70 + "\n"
            )
            logger.critical(f"Розмір черги перевищив критичний поріг: {self.critical_queue_size}")
            logger.critical(f"Поточний розмір черги: {self.request_queue.qsize()}")
            logger.critical("АДМІНІСТРАТОРУ: Необхідна негайна дія!")
            logger.critical("Варіанти розв'язання:")
            logger.critical("  1. Збільшити потужність БД")
            logger.critical("  2. Оптимізувати запити клієнтів")
            logger.critical("  3. Впровадити механізм дроселювання трафіку (rate limiting)")
            logger.critical("="*70 + "\n")
            
        except asyncio.CancelledError:
            logger.info("[QueueMonitor] Сторож зупинено.")

    # ========================================================================
    # ДОПОМІЖНІ МЕТОДИ
    # ========================================================================

    async def performance_monitor(self):
        """
        Додатковий компонент: виводить метрики продуктивності щосекунди.
        
        Показує:
        - Розмір черги
        - Частоту генерування запитів
        - Частоту обробки запитів
        """
        try:
            while self.is_running:
                await asyncio.sleep(2.0)
                
                queue_size = self.request_queue.qsize()
                percentage = (queue_size / self.max_queue_size) * 100
                
                status = "✅ НОРМА"
                if queue_size > self.critical_queue_size:
                    status = "⚠️  КРИТИЧНО!"
                elif queue_size > self.critical_queue_size * 0.7:
                    status = "⚠️  УВАГА"
                
                logger.info(
                    f"[Performance] {status} | "
                    f"Черга: {queue_size}/{self.max_queue_size} ({percentage:.1f}%) | "
                    f"Генеровано: {self.total_requests_generated} | "
                    f"Оброблено: {self.processed_count}"
                )
                
        except asyncio.CancelledError:
            logger.info("[Performance] Монітор продуктивності зупинено.")

    async def run(self, duration: Optional[float] = None):
        """
        Запуск всієї системи.
        
        Args:
            duration: час роботи системи у секундах (None = нескінченна робота)
        """
        logger.info("\n" + "="*70)
        logger.info("🚀 ЗАПУСК СИСТЕМИ МОНІТОРИНГУ ЧЕРГИ БД")
        logger.info("="*70 + "\n")
        
        # Створюємо всі асинхронні таски
        tasks = [
            asyncio.create_task(self.client_generator(request_rate=3.0)),
            asyncio.create_task(self.database_processor(processing_time=0.6)),
            asyncio.create_task(self.queue_monitor_watchdog()),
            asyncio.create_task(self.performance_monitor()),
        ]
        
        try:
            # Якщо задана тривалість, чекаємо на неї
            if duration:
                await asyncio.sleep(duration)
            else:
                # Чекаємо на критичну подію
                event_wait_task = asyncio.create_task(self.queue_overflow_event.wait())
                
                # Використовуємо wait() для реакції на першу завершену задачу
                done, pending = await asyncio.wait(
                    [event_wait_task],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Даємо системі час для визначення критичної ситуації
                await asyncio.sleep(3.0)
                
        except KeyboardInterrupt:
            logger.info("\n[Main] Отримано сигнал переривання (Ctrl+C)")
        
        finally:
            # ================================================================
            # GRACEFUL SHUTDOWN: коректне завершення роботи
            # ================================================================
            logger.info("\n" + "="*70)
            logger.info("🛑 ЗАВЕРШЕННЯ РОБОТИ СИСТЕМИ")
            logger.info("="*70 + "\n")
            
            self.is_running = False
            
            # Зупиняємо всі таски
            logger.info("[Main] Скасування всіх асинхронних завдань...")
            for task in tasks:
                task.cancel()
            
            # Чекаємо на завершення всіх тасків
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Логуємо результати завершення
            for i, result in enumerate(results):
                if isinstance(result, asyncio.CancelledError):
                    logger.info(f"[Main] Task #{i} коректно завершено")
                elif isinstance(result, Exception):
                    logger.error(f"[Main] Task #{i} завершено з помилкою: {result}")
            
            # Виводимо фінальну статистику
            logger.info("\n" + "="*70)
            logger.info("📈 ФІНАЛЬНА СТАТИСТИКА")
            logger.info("="*70)
            logger.info(f"Всього запитів сгенеровано: {self.total_requests_generated}")
            logger.info(f"Всього запитів оброблено: {self.processed_count}")
            logger.info(f"Запитів в черзі: {self.request_queue.qsize()}")
            logger.info(f"Критична подія спрацювала: {self.queue_overflow_event.is_set()}")
            logger.info("="*70 + "\n")
            
            logger.info("✅ Система коректно зупинена. До побачення!\n")


# ============================================================================
# ТОЧКА ВХОДУ
# ============================================================================

async def main():
    """Головна функція програми."""
    # Створюємо систему з критичним розміром черги 50 запитів
    system = DatabaseQueueMonitoringSystem(critical_queue_size=50, max_queue_size=100)
    
    # Запускаємо систему на 60 секунд (або до спрацювання критичної події)
    await system.run(duration=None)  # None означає "чекати критичну подію"


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
