"""
РОЗШИРЕНА ВЕРСІЯ з додатковими можливостями:
1. Timeout handling (asyncio.wait_for з таймаутом 3 сек)
2. Multiple Consumers (два споживачі: один логує у файл, другий аналізує)
3. Performance Metrics (детальна статистика щосекунди)
"""

import asyncio
import random
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
from collections import deque

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
# РОЗШИРЕНА СИСТЕМА З ДОДАТКОВИМИ МОЖЛИВОСТЯМИ
# ============================================================================

class AdvancedDatabaseQueueMonitoringSystem:
    """
    Розширена система моніторингу з:
    - Timeout handling
    - Multiple Consumers
    - Performance Metrics
    """
    
    def __init__(self, critical_queue_size: int = 35, max_queue_size: int = 100):
        # Основні компоненти
        self.request_queue = asyncio.Queue(maxsize=max_queue_size)
        self.queue_overflow_event = asyncio.Event()
        self.critical_queue_size = critical_queue_size
        self.max_queue_size = max_queue_size
        
        # Лічильники
        self.processed_count = 0
        self.total_requests_generated = 0
        self.timeout_count = 0
        self.is_running = True
        
        # Для логування в файл
        self.log_file = Path(__file__).parent / "queue_requests.log"
        self.log_file.write_text("")  # Очищуємо файл
        
        # Для метрик продуктивності
        self.metrics_history: Dict[str, deque] = {
            'queue_size': deque(maxlen=60),
            'requests_per_sec': deque(maxlen=60),
            'processed_per_sec': deque(maxlen=60),
            'timeout_count': deque(maxlen=60),
        }
        
        logger.info("=== РОЗШИРЕНА СИСТЕМА МОНІТОРИНГУ ЧЕРГИ БД ===")
        logger.info(f"Критичний розмір: {critical_queue_size}")
        logger.info(f"Логування запитів в: {self.log_file}")

    # ========================================================================
    # PRODUCER
    # ========================================================================

    async def client_generator(self, request_rate: float = 5.0):
        """Генератор запитів з вищою частотою."""
        try:
            request_id = 0
            generated_per_sec = 0
            second_start = asyncio.get_event_loop().time()
            
            while self.is_running:
                wait_time = random.expovariate(request_rate)
                await asyncio.sleep(wait_time)
                
                request_id += 1
                request_data = {
                    'id': request_id,
                    'query': f'SELECT * FROM users WHERE id = {random.randint(1, 1000)}',
                    'timestamp': datetime.now().isoformat(),
                    'client_ip': f'192.168.1.{random.randint(1, 255)}',
                    'query_type': random.choice(['SELECT', 'INSERT', 'UPDATE', 'DELETE'])
                }
                
                try:
                    self.request_queue.put_nowait(request_data)
                    self.total_requests_generated += 1
                    generated_per_sec += 1
                    
                    queue_size = self.request_queue.qsize()
                    
                    # Логуємо контрольні точки
                    if queue_size % 10 == 0:
                        logger.info(f"[Gen] Запит #{request_id} | Черга: {queue_size}/{self.max_queue_size}")
                    
                    # Перевіряємо критичний поріг
                    if queue_size > self.critical_queue_size and not self.queue_overflow_event.is_set():
                        self.queue_overflow_event.set()
                    
                    # Статистика за секунду
                    current_time = asyncio.get_event_loop().time()
                    if current_time - second_start >= 1.0:
                        self.metrics_history['requests_per_sec'].append(generated_per_sec)
                        logger.info(f"[Gen] 📨 {generated_per_sec} запитів/сек")
                        generated_per_sec = 0
                        second_start = current_time
                
                except asyncio.QueueFull:
                    logger.error("[Gen] Черга переповнена! Запит втрачено.")
                    
        except asyncio.CancelledError:
            logger.info("[Gen] Генератор зупинено.")

    # ========================================================================
    # MULTIPLE CONSUMERS
    # ========================================================================

    async def database_processor_analyzer(self):
        """
        Споживач #1: Аналізує запити на критичні стани.
        """
        processed_per_sec = 0
        second_start = asyncio.get_event_loop().time()
        
        try:
            while self.is_running:
                try:
                    # Timeout handling: чекаємо 3 секунди
                    request = await asyncio.wait_for(
                        self.request_queue.get(),
                        timeout=3.0
                    )
                    
                    # Імітуємо обробку
                    processing_time = random.uniform(0.3, 0.8)
                    await asyncio.sleep(processing_time)
                    
                    self.processed_count += 1
                    processed_per_sec += 1
                    
                    # Аналіз запиту
                    query_type = request['query_type']
                    if query_type in ['INSERT', 'UPDATE', 'DELETE']:
                        logger.warning(
                            f"[Analyzer] ⚠️  Запит MODIFICATION: {query_type} "
                            f"від {request['client_ip']}"
                        )
                    
                    self.request_queue.task_done()
                    
                    # Статистика за секунду
                    current_time = asyncio.get_event_loop().time()
                    if current_time - second_start >= 1.0:
                        self.metrics_history['processed_per_sec'].append(processed_per_sec)
                        processed_per_sec = 0
                        second_start = current_time
                
                except asyncio.TimeoutError:
                    self.timeout_count += 1
                    self.metrics_history['timeout_count'].append(1)
                    logger.warning(
                        f"[Analyzer] ⏱️  TIMEOUT! Черга була пуста 3 сек. "
                        f"(Всього: {self.timeout_count})"
                    )
                    
        except asyncio.CancelledError:
            logger.info("[Analyzer] Аналізатор зупинено.")

    async def database_processor_logger(self):
        """
        Споживач #2: Логує всі запити у файл.
        
        Демонструє можливість мати кількох споживачів однієї черги.
        """
        try:
            request_count = 0
            
            while self.is_running:
                try:
                    # Отримуємо з черги
                    request = await asyncio.wait_for(
                        self.request_queue.get(),
                        timeout=3.0
                    )
                    
                    # Логуємо у файл
                    log_entry = {
                        'request_id': request['id'],
                        'query_type': request['query_type'],
                        'client_ip': request['client_ip'],
                        'timestamp': request['timestamp'],
                        'processed_at': datetime.now().isoformat()
                    }
                    
                    # Додаємо у файл (append mode)
                    with open(self.log_file, 'a') as f:
                        f.write(json.dumps(log_entry) + '\n')
                    
                    request_count += 1
                    
                    if request_count % 5 == 0:
                        logger.info(
                            f"[Logger] 📝 {request_count} запитів залоговано в файл"
                        )
                    
                    self.request_queue.task_done()
                
                except asyncio.TimeoutError:
                    pass  # Logger не ропить помилки про timeout
                    
        except asyncio.CancelledError:
            logger.info(f"[Logger] Логгер зупинено. Всього залоговано запитів: {request_count}")

    # ========================================================================
    # MONITORS
    # ========================================================================

    async def queue_monitor_watchdog(self):
        """Сторож: чекає на критичну подію."""
        try:
            await self.queue_overflow_event.wait()
            
            logger.critical(
                "\n" + "="*70 + "\n"
                "🚨 КРИТИЧНА ПОДІЯ: ПЕРЕПОВНЕННЯ ЧЕРГИ! 🚨\n" +
                "="*70
            )
            logger.critical(f"Розмір черги: {self.request_queue.qsize()} > {self.critical_queue_size}")
            logger.critical("Необхідна негайна дія!")
            logger.critical("="*70 + "\n")
            
        except asyncio.CancelledError:
            logger.info("[Watchdog] Сторож зупинено.")

    async def performance_metrics_monitor(self):
        """
        Монітор продуктивності: детальна статистика щосекунди.
        
        Це розширена версія basic performance monitor з більш детальними метриками.
        """
        try:
            while self.is_running:
                await asyncio.sleep(2.0)
                
                queue_size = self.request_queue.qsize()
                self.metrics_history['queue_size'].append(queue_size)
                
                percentage = (queue_size / self.max_queue_size) * 100
                
                status = "✅ НОРМА"
                if queue_size > self.critical_queue_size:
                    status = "🚨 КРИТИЧНО!"
                elif queue_size > self.critical_queue_size * 0.7:
                    status = "⚠️  УВАГА"
                
                # Обчислюємо середні значення
                avg_queue = (
                    sum(self.metrics_history['queue_size']) / 
                    len(self.metrics_history['queue_size'])
                    if self.metrics_history['queue_size'] else 0
                )
                
                avg_generated = (
                    sum(self.metrics_history['requests_per_sec']) / 
                    len(self.metrics_history['requests_per_sec'])
                    if self.metrics_history['requests_per_sec'] else 0
                )
                
                avg_processed = (
                    sum(self.metrics_history['processed_per_sec']) / 
                    len(self.metrics_history['processed_per_sec'])
                    if self.metrics_history['processed_per_sec'] else 0
                )
                
                logger.info(
                    f"[Perf] {status}\n"
                    f"       Черга: {queue_size}/{self.max_queue_size} ({percentage:.0f}%) | "
                    f"Середня: {avg_queue:.1f}\n"
                    f"       Генеровано: {self.total_requests_generated} | "
                    f"Сер/сек: {avg_generated:.1f}\n"
                    f"       Оброблено: {self.processed_count} | "
                    f"Сер/сек: {avg_processed:.1f} | "
                    f"Timeouts: {self.timeout_count}"
                )
                
        except asyncio.CancelledError:
            logger.info("[Perf] Монітор метрик зупинено.")

    # ========================================================================
    # ЗАПУСК СИСТЕМИ
    # ========================================================================

    async def run(self):
        """Запуск розширеної системи."""
        logger.info("\n" + "="*70)
        logger.info("🚀 ЗАПУСК РОЗШИРЕНОЇ СИСТЕМИ")
        logger.info("="*70 + "\n")
        
        # Створюємо таски для всіх компонентів
        tasks = [
            asyncio.create_task(self.client_generator(request_rate=5.0)),
            # Два споживачі:
            asyncio.create_task(self.database_processor_analyzer()),
            asyncio.create_task(self.database_processor_logger()),
            # Монітори:
            asyncio.create_task(self.queue_monitor_watchdog()),
            asyncio.create_task(self.performance_metrics_monitor()),
        ]
        
        try:
            event_wait = asyncio.create_task(self.queue_overflow_event.wait())
            await asyncio.wait([event_wait], return_when=asyncio.FIRST_COMPLETED)
            await asyncio.sleep(3.0)
            
        except KeyboardInterrupt:
            logger.info("\n[Main] Переривання користувачем")
        
        finally:
            logger.info("\n" + "="*70)
            logger.info("🛑 ЗАВЕРШЕННЯ РОБОТИ")
            logger.info("="*70 + "\n")
            
            self.is_running = False
            
            for task in tasks:
                task.cancel()
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Фінальна статистика
            logger.info("\n" + "="*70)
            logger.info("📈 ФІНАЛЬНА СТАТИСТИКА")
            logger.info("="*70)
            logger.info(f"Запитів генеровано: {self.total_requests_generated}")
            logger.info(f"Запитів оброблено: {self.processed_count}")
            logger.info(f"Запитів в черзі: {self.request_queue.qsize()}")
            logger.info(f"Timeout подій: {self.timeout_count}")
            logger.info(f"Критична подія: {'ТАК ✓' if self.queue_overflow_event.is_set() else 'НІ'}")
            logger.info(f"Запити залогована в: {self.log_file}")
            
            # Показуємо вміст файлу логів
            if self.log_file.exists():
                log_lines = self.log_file.read_text().strip().split('\n')
                logger.info(f"Всього записів у файлі логів: {len(log_lines)}")
                if log_lines:
                    logger.info("Перші 3 записи з логу:")
                    for line in log_lines[:3]:
                        logger.info(f"  {line}")
            
            logger.info("="*70 + "\n")
            logger.info("✅ Система коректно зупинена.\n")


# ============================================================================
# ТОЧКА ВХОДУ
# ============================================================================

async def main():
    """Запуск розширеної системи."""
    system = AdvancedDatabaseQueueMonitoringSystem(
        critical_queue_size=35,
        max_queue_size=100
    )
    await system.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
