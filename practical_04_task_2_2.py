"""
ПР №4: Розробка мультипоточних додатків з використанням синхронізації в Python.
Завдання 2.2: Семафори та Event для управління конкурентним доступом.
"""

import random
import threading
import time


class ConnectionPool:
    """Пул з'єднань з обмеженням через Semaphore та захистом статистики через Lock."""

    def __init__(self, max_connections: int = 3):
        self._semaphore = threading.Semaphore(max_connections)
        self._lock = threading.Lock()
        self.max_connections = max_connections
        self.active_connections = 0
        self.total_served = 0
        self.peak_connections = 0

    def acquire_connection(self, client_name: str) -> None:
        """Отримання з'єднання (блокується, якщо всі з'єднання пулу зайняті)."""
        print(f"[{client_name}] Запит на з'єднання...")
        # Семафор контролює кількість одночасних доступів
        self._semaphore.acquire()

        # Захищаємо оновлення лічильників замком Lock
        with self._lock:
            self.active_connections += 1
            if self.active_connections > self.peak_connections:
                self.peak_connections = self.active_connections
            # Перевірка інваріанта безпеки
            assert self.active_connections <= self.max_connections, (
                f"Помилка: активних з'єднань {self.active_connections} > макс {self.max_connections}"
            )
            print(f"  --> [{client_name}] З'ЄДНАННЯ ОТРИМАНО. "
                  f"(Активних у пулі: {self.active_connections}/{self.max_connections})")

    def release_connection(self, client_name: str) -> None:
        """Повернення з'єднання до пулу."""
        with self._lock:
            self.active_connections -= 1
            self.total_served += 1
            print(f"  <-- [{client_name}] З'єднання повернуто. "
                  f"(Залишилось активних: {self.active_connections}/{self.max_connections})")

        # Вивільняємо семафор для наступного очікуючого клієнта
        self._semaphore.release()

    def get_stats(self) -> dict:
        """Повертає статистику використання пулу."""
        with self._lock:
            return {
                "max": self.max_connections,
                "active": self.active_connections,
                "total_served": self.total_served,
                "peak": self.peak_connections,
            }


def client_task(pool: ConnectionPool, client_name: str,
                ready_event: threading.Event) -> None:
    """Клієнт, що чекає сигналу готовності сервера та використовує з'єднання."""
    # Очікуємо загального сигналу готовності сервера (бар'єр старту)
    ready_event.wait()

    pool.acquire_connection(client_name)
    try:
        work_time = random.uniform(0.3, 0.8)
        print(f"      [{client_name}] Виконує запит до БД протягом {work_time:.2f} сек...")
        time.sleep(work_time)
    finally:
        pool.release_connection(client_name)


def main():
    max_conns = 3
    num_clients = 10
    pool = ConnectionPool(max_connections=max_conns)
    ready_event = threading.Event()

    print("=" * 60)
    print("=== Завдання 2.2: Семафори та Event ===")
    print("=" * 60)
    print(f"Максимальна місткість пулу (Semaphore): {max_conns}")
    print(f"Кількість підключених клієнтів:        {num_clients}")
    print("Створення та підготовка клієнтських потоків (очікують ready_event)...")

    threads = []
    for i in range(num_clients):
        t = threading.Thread(
            target=client_task,
            args=(pool, f"Client-{i + 1:02d}", ready_event),
            name=f"Thread-Client-{i + 1:02d}",
        )
        threads.append(t)
        t.start()

    # Даємо потокам час стартувати та перейти в режим wait()
    time.sleep(0.5)
    print("\n>>> Сервер готовий! Сигнал ready_event.set() відправлено.\n"
          ">>> Усі клієнти одночасно починають змагання за з'єднання.\n")
    ready_event.set()

    # Очікуємо завершення всіх потоків
    for t in threads:
        t.join()

    # Виводимо статистику пулу
    stats = pool.get_stats()
    print("\n" + "=" * 40)
    print("=== Підсумкова статистика пулу ===")
    print("=" * 40)
    print(f"Макс. допустимих з'єднань:  {stats['max']}")
    print(f"Пікове навантаження (peak): {stats['peak']}")
    print(f"Обслуговано клієнтів:       {stats['total_served']} з {num_clients}")
    print(f"Активних на момент кінця:   {stats['active']}")
    print(f"Інваріант peak <= max:      {'ДОТРИМАНО' if stats['peak'] <= stats['max'] else 'ПОРУШЕНО'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
