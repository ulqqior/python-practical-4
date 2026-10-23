"""
ПР №4: Розробка мультипоточних додатків з використанням синхронізації в Python.
Завдання 1.1: Створення та керування потоками в Python (threading).
"""

import threading
import time


def download_simulation(file_name: str, size_mb: float) -> dict:
    """Імітація завантаження файлу (I/O-bound задача).

    Args:
        file_name: назва файлу.
        size_mb: розмір файлу в мегабайтах.

    Returns:
        Словник з інформацією про завантажений файл.
    """
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Початок завантаження '{file_name}' ({size_mb} MB)...")

    # Імітуємо час завантаження пропорційно розміру файлу (0.25 сек на 1 MB)
    download_time = size_mb * 0.25
    time.sleep(download_time)

    print(f"[{thread_name}] Завершено завантаження '{file_name}' за {download_time:.2f} сек.")
    return {"file": file_name, "size": size_mb, "thread": thread_name}


class MonitorThread(threading.Thread):
    """Потік-монітор, що періодично виводить стан системи."""

    def __init__(self, interval: float = 0.5):
        super().__init__(name="Monitor", daemon=True)
        self.interval = interval
        self.active_downloads = 0
        self._stop_event = threading.Event()

    def run(self):
        """Виводить кількість активних потоків кожні `interval` секунд."""
        while not self._stop_event.is_set():
            # threading.active_count() показує загальну кількість живих потоків
            active_cnt = threading.active_count()
            print(f"  [Monitor] Активних потоків у системі: {active_cnt}")
            # Очікуємо інтервал або сигнал зупинки
            self._stop_event.wait(timeout=self.interval)

    def stop(self):
        """Встановлює подію для зупинки монітора."""
        self._stop_event.set()


def main():
    files = [
        ("data.csv", 2.5),
        ("report.pdf", 1.0),
        ("image.png", 0.5),
        ("archive.zip", 4.0),
        ("log.txt", 0.3),
    ]

    print("=" * 60)
    print("=== Завдання 1.1: Демонстрація модуля threading ===")
    print("=" * 60)

    # === 1. Послідовне виконання для порівняння ===
    print("\n>>> 1. Послідовне завантаження файлів:")
    seq_start = time.perf_counter()
    for fname, fsize in files:
        download_simulation(fname, fsize)
    seq_elapsed = time.perf_counter() - seq_start
    print(f"Послідовне виконання зайняло: {seq_elapsed:.2f} сек\n")

    # === 2. Паралельне виконання з MonitorThread ===
    print(">>> 2. Паралельне завантаження файлів з MonitorThread (daemon):")
    # Створюємо та запускаємо демон-потік моніторингу
    monitor = MonitorThread(interval=0.4)
    monitor.start()

    # Створюємо список робочих потоків
    download_threads = []
    results = []

    def worker_wrapper(fname, fsize):
        res = download_simulation(fname, fsize)
        results.append(res)

    for i, (fname, fsize) in enumerate(files, start=1):
        t = threading.Thread(
            target=worker_wrapper,
            args=(fname, fsize),
            name=f"Downloader-{i}",
        )
        download_threads.append(t)

    # Запускаємо потоки та вимірюємо час паралельного виконання
    par_start = time.perf_counter()
    for t in download_threads:
        t.start()

    # Очікуємо завершення всіх потоків завантаження
    for t in download_threads:
        t.join()

    par_elapsed = time.perf_counter() - par_start

    # Зупиняємо потік-монітор
    monitor.stop()
    monitor.join(timeout=1.0)

    print(f"\nУсі файли завантажено паралельно за {par_elapsed:.2f} сек")
    print(f"Кількість завантажених файлів: {len(results)}")
    speedup = seq_elapsed / par_elapsed if par_elapsed > 0 else 1.0
    print(f"Прискорення: {speedup:.2f}x")
    print("=" * 60)


if __name__ == "__main__":
    main()
