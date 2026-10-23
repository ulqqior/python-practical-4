"""
ПР №4: Розробка мультипоточних додатків з використанням синхронізації в Python.
Завдання 3.2: Черги для міжпотокової комунікації (Producer-Consumer) та дедлоки.
"""

import queue
import random
import threading
import time


class Task:
    """Завдання для обробки."""

    def __init__(self, task_id: int, data: str, priority: int = 0):
        self.task_id = task_id
        self.data = data
        self.priority = priority
        self.created_at = time.time()

    def __repr__(self):
        return f"Task(id={self.task_id}, data='{self.data}')"


def producer(task_queue: queue.Queue, num_tasks: int,
             producer_id: int) -> None:
    """Виробник: генерує завдання та поміщає їх у чергу."""
    for i in range(num_tasks):
        task = Task(
            task_id=producer_id * 100 + i,
            data=f"Пакет_{i} від Виробника_{producer_id}",
            priority=random.randint(1, 5),
        )
        # Блокується, якщо черга заповнена (зворотний тиск / backpressure, maxsize)
        task_queue.put(task)
        print(f"[Виробник-{producer_id}] Створено та додано {task} (у черзі: {task_queue.qsize()})")
        time.sleep(random.uniform(0.05, 0.15))


def consumer(task_queue: queue.Queue, consumer_id: int,
             results: list, results_lock: threading.Lock) -> None:
    """Споживач: обробляє завдання з черги."""
    while True:
        try:
            # Отримання задачі з тайм-аутом для запобігання вічному зависанню
            task = task_queue.get(timeout=2)
        except queue.Empty:
            print(f"  [Споживач-{consumer_id}] Черга порожня (тайм-аут), завершення")
            break

        if task is None:
            # Передати sentinel-значення іншим споживачам для каскадного завершення
            task_queue.task_done()
            task_queue.put(None)
            print(f"  [Споживач-{consumer_id}] Отримано сигнал завершення (sentinel None)")
            break

        # Імітація обробки задачі
        proc_time = random.uniform(0.1, 0.25)
        time.sleep(proc_time)
        latency = time.time() - task.created_at

        with results_lock:
            results.append({
                "task_id": task.task_id,
                "consumer_id": consumer_id,
                "processing_time": proc_time,
                "latency": latency,
            })

        print(f"  [Споживач-{consumer_id}] Оброблено Task {task.task_id} за {proc_time:.2f} сек "
              f"(латентність: {latency:.2f} сек)")
        task_queue.task_done()


def producer_consumer_demo():
    """Демонстрація паттерну Producer-Consumer."""
    task_queue = queue.Queue(maxsize=5)
    results = []
    results_lock = threading.Lock()
    num_producers = 2
    num_consumers = 3
    tasks_per_producer = 5

    print(f"Конфігурація: Виробників: {num_producers}, Споживачів: {num_consumers}, "
          f"Задач на виробника: {tasks_per_producer}, Місткість черги: {task_queue.maxsize}")

    # Створюємо та запускаємо споживачів
    consumer_threads = []
    for cid in range(1, num_consumers + 1):
        ct = threading.Thread(
            target=consumer,
            args=(task_queue, cid, results, results_lock),
            name=f"Consumer-{cid}",
        )
        consumer_threads.append(ct)
        ct.start()

    # Створюємо та запускаємо виробників
    producer_threads = []
    for pid in range(1, num_producers + 1):
        pt = threading.Thread(
            target=producer,
            args=(task_queue, tasks_per_producer, pid),
            name=f"Producer-{pid}",
        )
        producer_threads.append(pt)
        pt.start()

    # Очікуємо завершення роботи виробників
    for pt in producer_threads:
        pt.join()

    # Посилаємо sentinel None для запуску каскадного завершення споживачів
    task_queue.put(None)

    # Очікуємо завершення споживачів
    for ct in consumer_threads:
        ct.join()

    # Статистика результатів
    total_processed = len(results)
    avg_latency = (
        sum(r["latency"] for r in results) / total_processed
        if total_processed > 0 else 0
    )
    avg_proc = (
        sum(r["processing_time"] for r in results) / total_processed
        if total_processed > 0 else 0
    )

    print("\n" + "=" * 40)
    print("=== Підсумок Producer-Consumer ===")
    print("=" * 40)
    print(f"Загалом успішно оброблено задач: {total_processed} з {num_producers * tasks_per_producer}")
    print(f"Середній час обробки задачі:      {avg_proc:.3f} сек")
    print(f"Середній загальний час у черзі:   {avg_latency:.3f} сек")


def deadlock_demo():
    """Демонстрація та виявлення потенційного дедлоку."""
    lock_a = threading.Lock()
    lock_b = threading.Lock()
    deadlock_detected = threading.Event()

    def worker_1():
        """Захоплює lock_a, потім lock_b."""
        lock_a.acquire()
        print("[Worker-1] Захопив Lock A, пауза для перемикання контексту...")
        time.sleep(0.15)
        print("[Worker-1] Намагається захопити Lock B (тайм-аут 2 сек)...")
        acquired = lock_b.acquire(timeout=2)
        if not acquired:
            print("[Worker-1] ДЕДЛОК ВИЯВЛЕНО: не вдалося захопити Lock B!")
            deadlock_detected.set()
            lock_a.release()
            return
        print("[Worker-1] Захопив Lock B")
        lock_b.release()
        lock_a.release()

    def worker_2():
        """Захоплює lock_b, потім lock_a — зворотний порядок!"""
        lock_b.acquire()
        print("[Worker-2] Захопив Lock B, пауза для перемикання контексту...")
        time.sleep(0.15)
        print("[Worker-2] Намагається захопити Lock A (тайм-аут 2 сек)...")
        acquired = lock_a.acquire(timeout=2)
        if not acquired:
            print("[Worker-2] ДЕДЛОК ВИЯВЛЕНО: не вдалося захопити Lock A!")
            deadlock_detected.set()
            lock_b.release()
            return
        print("[Worker-2] Захопив Lock A")
        lock_a.release()
        lock_b.release()

    def safe_worker_1():
        """Безпечна версія: завжди захоплює A -> B."""
        with lock_a:
            print("  [SafeWorker-1] Захопив Lock A")
            time.sleep(0.05)
            with lock_b:
                print("  [SafeWorker-1] Захопив Lock B -> успішно виконав роботу!")

    def safe_worker_2():
        """Безпечна версія: той самий порядок A -> B."""
        with lock_a:
            print("  [SafeWorker-2] Захопив Lock A")
            time.sleep(0.05)
            with lock_b:
                print("  [SafeWorker-2] Захопив Lock B -> успішно виконав роботу!")

    # === 1. Демонстрація дедлоку ===
    print("\n--- 1. Спроба з потенційним дедлоком (зворотний порядок замків) ---")
    t1 = threading.Thread(target=worker_1)
    t2 = threading.Thread(target=worker_2)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    if deadlock_detected.is_set():
        print(">>> Підтвердження: Дедлок успішно виявлено за тайм-аутом 2.0 сек.")
        print(">>> Потоки не зависли назавжди завдяки lock.acquire(timeout=2)!\n")
    else:
        print(">>> Дедлок не виник цього разу (залежить від планувальника).\n")

    # === 2. Безпечна версія ===
    print("--- 2. Безпечна версія (строго однаковий порядок захоплення замків: Lock A -> Lock B) ---")
    st1 = threading.Thread(target=safe_worker_1)
    st2 = threading.Thread(target=safe_worker_2)
    st1.start()
    st2.start()
    st1.join()
    st2.join()
    print(">>> Безпечна версія завершилась миттєво та без жодних блокувань!")


def main():
    print("=" * 60)
    print("=== Завдання 3.2: Черги (queue.Queue) та виявлення дедлоків ===")
    print("=" * 60)

    print("\n" + "-" * 50)
    print("=== Паттерн Producer-Consumer ===")
    print("-" * 50)
    producer_consumer_demo()

    print("\n" + "-" * 50)
    print("=== Демонстрація дедлоку та його усунення ===")
    print("-" * 50)
    deadlock_demo()
    print("=" * 60)


if __name__ == "__main__":
    main()
