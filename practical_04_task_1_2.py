"""
ПР №4: Розробка мультипоточних додатків з використанням синхронізації в Python.
Завдання 1.2: Паралельне обчислення з multiprocessing (CPU-bound задача).
"""

import multiprocessing
import os
import time


def is_prime(n: int) -> bool:
    """Перевірка числа на простоту."""
    if n < 2:
        return False
    if n < 4:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


def sum_primes_in_range(start: int, end: int) -> tuple[int, int, int]:
    """Обчислює суму та кількість простих чисел у діапазоні [start, end).

    Returns:
        Кортеж (сума простих, кількість простих, PID процесу).
    """
    pid = os.getpid()
    total_sum = 0
    count = 0
    for n in range(start, end):
        if is_prime(n):
            total_sum += n
            count += 1
    return total_sum, count, pid


def main():
    total_range = 1_000_000
    num_workers = 4

    print("=" * 60)
    print("=== Завдання 1.2: Паралельне обчислення з multiprocessing ===")
    print("=" * 60)
    print(f"Діапазон обчислення: [2, {total_range})")
    print(f"Кількість робочих процесів: {num_workers}")

    # Розбиття діапазону [2, total_range) на num_workers частин
    chunk_size = total_range // num_workers
    ranges = []
    for i in range(num_workers):
        start = i * chunk_size if i > 0 else 2
        end = (i + 1) * chunk_size if i < num_workers - 1 else total_range
        ranges.append((start, end))

    print(f"Діапазони розбиття: {ranges}")

    # === Послідовне виконання ===
    print("\n>>> 1. Послідовне виконання:")
    start_time = time.perf_counter()
    seq_results = []
    for r in ranges:
        res = sum_primes_in_range(r[0], r[1])
        seq_results.append(res)
    seq_time = time.perf_counter() - start_time

    seq_total_sum = sum(res[0] for res in seq_results)
    seq_total_count = sum(res[1] for res in seq_results)
    seq_pids = set(res[2] for res in seq_results)

    print(f"Послідовно знайдено простих: {seq_total_count}")
    print(f"Сума простих: {seq_total_sum}")
    print(f"PID процесу послідовного виконання: {seq_pids}")
    print(f"Час послідовного виконання: {seq_time:.3f} сек")

    # === Паралельне виконання з multiprocessing.Pool ===
    print(f"\n>>> 2. Паралельне виконання (multiprocessing.Pool, {num_workers} processes):")
    start_time = time.perf_counter()
    with multiprocessing.Pool(processes=num_workers) as pool:
        par_results = pool.starmap(sum_primes_in_range, ranges)
    par_time = time.perf_counter() - start_time

    par_total_sum = sum(res[0] for res in par_results)
    par_total_count = sum(res[1] for res in par_results)

    print("Результати по процесах:")
    for idx, (p_sum, p_cnt, p_pid) in enumerate(par_results):
        r_start, r_end = ranges[idx]
        print(f"  Воркер {idx + 1} (PID: {p_pid}): діапазон [{r_start}, {r_end}) -> "
              f"кількість = {p_cnt}, сума = {p_sum}")

    print(f"\nЗагальна кількість простих: {par_total_count}")
    print(f"Загальна сума простих: {par_total_sum}")
    print(f"Унікальні PID процесів: {set(res[2] for res in par_results)}")

    # Перевірка збігу результатів
    assert seq_total_sum == par_total_sum, "Помилка: суми не збігаються!"
    assert seq_total_count == par_total_count, "Помилка: кількості не збігаються!"

    print("\n=== Порівняння продуктивності ===")
    print(f"Послідовно: {seq_time:.3f} сек")
    print(f"Паралельно ({num_workers} процеси): {par_time:.3f} сек")
    speedup = seq_time / par_time if par_time > 0 else 1.0
    print(f"Прискорення: {speedup:.2f}x")
    print("=" * 60)


if __name__ == "__main__":
    main()
