"""
ПР №4: Розробка мультипоточних додатків з використанням синхронізації в Python.
Завдання 3.1: Порівняння ThreadPoolExecutor та ProcessPoolExecutor (concurrent.futures).
"""

from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
import hashlib
import time


def io_bound_task(url: str) -> dict:
    """Імітація I/O-bound задачі (завантаження сторінки)."""
    # Імітуємо очікування мережевої відповіді
    time.sleep(0.2)
    return {"url": url, "status": 200, "size": len(url) * 42}


def cpu_bound_task(n: int) -> dict:
    """CPU-bound задача: обчислення хеш-суми великого діапазону."""
    hasher = hashlib.sha256()
    for i in range(n):
        hasher.update(str(i).encode())
    return {"n": n, "hash": hasher.hexdigest()[:16]}


def risky_task(value: int) -> float:
    """Задача, що може згенерувати виключення."""
    if value == 0:
        raise ZeroDivisionError("Спроба ділення на нуль!")
    if value < 0:
        raise ValueError(f"Від'ємне значення аргументу: {value}")
    return 1000.0 / value


def benchmark_io_bound():
    """Порівняння послідовного та ThreadPoolExecutor для I/O-bound."""
    urls = [f"https://api.example.com/data/{i}" for i in range(20)]
    workers = 5
    print(f"Кількість I/O задач (URL-запитів): {len(urls)}, потоків у пулі: {workers}")

    # 1. Послідовне виконання
    start_seq = time.perf_counter()
    seq_results = [io_bound_task(u) for u in urls]
    time_seq = time.perf_counter() - start_seq
    print(f"Послідовне виконання: {time_seq:.3f} сек (оброблено {len(seq_results)} запитів)")

    # 2. ThreadPoolExecutor з використанням executor.map()
    start_pool = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pool_results = list(executor.map(io_bound_task, urls))
    time_pool = time.perf_counter() - start_pool
    print(f"ThreadPoolExecutor (map): {time_pool:.3f} сек (оброблено {len(pool_results)} запитів)")

    speedup = time_seq / time_pool if time_pool > 0 else 1.0
    print(f"Прискорення ThreadPoolExecutor: {speedup:.2f}x")


def benchmark_cpu_bound():
    """Порівняння послідовного та ProcessPoolExecutor для CPU-bound."""
    tasks = [400_000] * 8
    workers = 4
    print(f"Кількість CPU задач (хешування 400k значень): {len(tasks)}, процесів у пулі: {workers}")

    # 1. Послідовне виконання
    start_seq = time.perf_counter()
    seq_results = [cpu_bound_task(t) for t in tasks]
    time_seq = time.perf_counter() - start_seq
    print(f"Послідовне виконання: {time_seq:.3f} сек")

    # 2. ProcessPoolExecutor з використанням executor.map()
    start_proc = time.perf_counter()
    with ProcessPoolExecutor(max_workers=workers) as executor:
        proc_results = list(executor.map(cpu_bound_task, tasks))
    time_proc = time.perf_counter() - start_proc
    print(f"ProcessPoolExecutor (map): {time_proc:.3f} сек")

    speedup = time_seq / time_proc if time_proc > 0 else 1.0
    print(f"Прискорення ProcessPoolExecutor: {speedup:.2f}x")


def handle_exceptions():
    """Демонстрація обробки виключень у concurrent.futures з submit() та as_completed()."""
    values = [10, 5, 0, -3, 8, 2]
    print(f"Вхідний масив для тестування виключень: {values}")

    with ThreadPoolExecutor(max_workers=3) as executor:
        # Реєструємо задачі через executor.submit() та зберігаємо асоціацію Future -> аргумент
        future_to_val = {executor.submit(risky_task, v): v for v in values}

        print("\nОбробка результатів у міру їх надходження (as_completed):")
        for future in as_completed(future_to_val):
            val = future_to_val[future]
            try:
                result = future.result()
                print(f"  [УСПІХ] risky_task({val:2d}) = {result:8.2f}")
            except ZeroDivisionError as e:
                print(f"  [ПОМИЛКА - ZeroDivisionError] risky_task({val:2d}) -> {e}")
            except ValueError as e:
                print(f"  [ПОМИЛКА - ValueError]        risky_task({val:2d}) -> {e}")
            except Exception as e:
                print(f"  [НЕВІДОМА ПОМИЛКА]            risky_task({val:2d}) -> {e}")


def main():
    print("=" * 60)
    print("=== Завдання 3.1: concurrent.futures (потоки та процеси) ===")
    print("=" * 60)

    print("\n" + "-" * 50)
    print("=== 1. I/O-bound: ThreadPoolExecutor ===")
    print("-" * 50)
    benchmark_io_bound()

    print("\n" + "-" * 50)
    print("=== 2. CPU-bound: ProcessPoolExecutor ===")
    print("-" * 50)
    benchmark_cpu_bound()

    print("\n" + "-" * 50)
    print("=== 3. Обробка виключень у пулах (submit + as_completed) ===")
    print("-" * 50)
    handle_exceptions()
    print("=" * 60)


if __name__ == "__main__":
    main()
