"""
Версія для демонстрації КРИТИЧНОЇ ÉVÉNІ
(Прискорена генерація запитів)
"""

import asyncio
import random
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class DatabaseQueueMonitoringSystemDemo:
    """Демонстраційна версія для показу критичної ситуації."""
    
    def __init__(self, critical_queue_size: int = 25, max_queue_size: int = 100):
        self.request_queue = asyncio.Queue(maxsize=max_queue_size)
        self.queue_overflow_event = asyncio.Event()
        self.critical_queue_size = critical_queue_size
        self.max_queue_size = max_queue_size
        
        self.processed_count = 0
        self.total_requests_generated = 0
        self.is_running = True
        
        logger.info("=== ДЕМО: Система моніторингу черги БД ===")
        logger.info(f"Критичний розмір черги: {critical_queue_size} запитів")
        logger.info(f"(На демонстрацію установлено нижче для швидкого показу)")

    async def client_generator(self, request_rate: float = 8.0):
        """Продюсер: генерує запити з вищою частотою."""
        try:
            request_id = 0
            while self.is_running:
                wait_time = random.expovariate(request_rate)
                await asyncio.sleep(wait_time)
                
                request_id += 1
                request_data = {
                    'id': request_id,
                    'query': f'SELECT * FROM users WHERE id = {random.randint(1, 1000)}',
                    'timestamp': datetime.now(),
                    'client_ip': f'192.168.1.{random.randint(1, 255)}'
                }
                
                try:
                    self.request_queue.put_nowait(request_data)
                    self.total_requests_generated += 1
                    
                    queue_size = self.request_queue.qsize()
                    logger.info(
                        f"[Gen] #{request_id} від {request_data['client_ip']} | "
                        f"Черга: {queue_size}/{self.max_queue_size}"
                    )
                    
                    if queue_size > self.critical_queue_size and not self.queue_overflow_event.is_set():
                        logger.warning(
                            f"[Gen] ⚠️ КРИТИЧНА! Черга ({queue_size}) > {self.critical_queue_size}"
                        )
                        self.queue_overflow_event.set()
                
                except asyncio.QueueFull:
                    logger.error("[Gen] Черга переповнена!")
                    
        except asyncio.CancelledError:
            logger.info("[Gen] Генератор зупинено.")

    async def database_processor(self, processing_time: float = 1.5):
        """Споживач: обробляє повільніше для накопичення черги."""
        requests_per_second = 0
        second_start = asyncio.get_event_loop().time()
        
        try:
            while self.is_running:
                try:
                    request = await asyncio.wait_for(
                        self.request_queue.get(),
                        timeout=1.0
                    )
                    
                    processing_delay = random.uniform(
                        max(0.1, processing_time - 0.3),
                        processing_time + 0.3
                    )
                    await asyncio.sleep(processing_delay)
                    
                    self.processed_count += 1
                    requests_per_second += 1
                    queue_size = self.request_queue.qsize()
                    
                    logger.info(
                        f"[Processor] #{request['id']} ✓ | "
                        f"Залишилось: {queue_size}"
                    )
                    
                    self.request_queue.task_done()
                    
                    current_time = asyncio.get_event_loop().time()
                    if current_time - second_start >= 1.0:
                        logger.info(
                            f"[Processor] 📊 {requests_per_second} запитів/сек | "
                            f"Всього: {self.processed_count}"
                        )
                        requests_per_second = 0
                        second_start = current_time
                
                except asyncio.TimeoutError:
                    current_time = asyncio.get_event_loop().time()
                    if current_time - second_start >= 1.0:
                        logger.info(
                            f"[Processor] 📊 {requests_per_second} запитів/сек | "
                            f"Всього: {self.processed_count}"
                        )
                        requests_per_second = 0
                        second_start = current_time
                        
        except asyncio.CancelledError:
            logger.info("[Processor] Обробник зупинено.")

    async def queue_monitor_watchdog(self):
        """Монітор: чекає на критичну подію."""
        try:
            logger.info("[Watchdog] Очікування на критичну подію...")
            
            await self.queue_overflow_event.wait()
            
            logger.critical(
                "\n" + "="*70 + "\n"
                "🚨 КРИТИЧНА ПОДІЯ: ПЕРЕПОВНЕННЯ ЧЕРГИ! 🚨\n"
                "="*70
            )
            logger.critical(f"Розмір черги: {self.request_queue.qsize()} > {self.critical_queue_size}")
            logger.critical("⚡ НЕГАЙНА ДІЯ ПОТРІБНА!")
            logger.critical("="*70 + "\n")
            
        except asyncio.CancelledError:
            logger.info("[Watchdog] Сторож зупинено.")

    async def performance_monitor(self):
        """Монітор продуктивності."""
        try:
            while self.is_running:
                await asyncio.sleep(2.0)
                
                queue_size = self.request_queue.qsize()
                percentage = (queue_size / self.max_queue_size) * 100
                
                status = "✅ НОРМА"
                if queue_size > self.critical_queue_size:
                    status = "🚨 КРИТИЧНО!"
                elif queue_size > self.critical_queue_size * 0.6:
                    status = "⚠️  УВАГА"
                
                logger.info(
                    f"[Perf] {status} | Черга: {queue_size}/{self.max_queue_size} "
                    f"({percentage:.0f}%) | Обр: {self.processed_count}"
                )
                
        except asyncio.CancelledError:
            logger.info("[Perf] Монітор зупинено.")

    async def run(self):
        """Запуск системи."""
        logger.info("\n" + "="*70)
        logger.info("🚀 ЗАПУСК ДЕМОНСТРАЦІЙНОЇ СИСТЕМИ")
        logger.info("="*70 + "\n")
        
        tasks = [
            asyncio.create_task(self.client_generator(request_rate=8.0)),
            asyncio.create_task(self.database_processor(processing_time=1.5)),
            asyncio.create_task(self.queue_monitor_watchdog()),
            asyncio.create_task(self.performance_monitor()),
        ]
        
        try:
            event_wait_task = asyncio.create_task(self.queue_overflow_event.wait())
            done, pending = await asyncio.wait(
                [event_wait_task],
                return_when=asyncio.FIRST_COMPLETED
            )
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
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
            logger.info("\n" + "="*70)
            logger.info("📈 ФІНАЛЬНА СТАТИСТИКА")
            logger.info("="*70)
            logger.info(f"Запитів генеровано: {self.total_requests_generated}")
            logger.info(f"Запитів оброблено: {self.processed_count}")
            logger.info(f"Запитів в черзі: {self.request_queue.qsize()}")
            logger.info(f"Критична подія: {'ТАК ✓' if self.queue_overflow_event.is_set() else 'НІ'}")
            logger.info("="*70 + "\n")


async def main():
    """Точка входу для демонстрації."""
    system = DatabaseQueueMonitoringSystemDemo(critical_queue_size=25, max_queue_size=100)
    await system.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted")
