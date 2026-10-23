"""
ПР №4: Розробка мультипоточних додатків з використанням синхронізації в Python.
Завдання 2.1: Демонстрація race condition та захист через Lock.
"""

import random
import threading
import time


class BankAccount:
    """Банківський рахунок без синхронізації (небезпечний)."""

    def __init__(self, balance: float = 0.0):
        self.balance = balance
        self.transaction_count = 0

    def deposit(self, amount: float) -> None:
        """Поповнення рахунку (неатомарна операція)."""
        current = self.balance
        # Імітація перемикання контексту ОС під час операції
        time.sleep(random.uniform(0.0005, 0.002))
        self.balance = current + amount
        cnt = self.transaction_count
        time.sleep(random.uniform(0.0001, 0.0005))
        self.transaction_count = cnt + 1

    def withdraw(self, amount: float) -> bool:
        """Зняття коштів (неатомарна операція)."""
        current = self.balance
        time.sleep(random.uniform(0.0005, 0.002))
        if current >= amount:
            self.balance = current - amount
            cnt = self.transaction_count
            time.sleep(random.uniform(0.0001, 0.0005))
            self.transaction_count = cnt + 1
            return True
        return False


class SafeBankAccount:
    """Банківський рахунок із синхронізацією через Lock."""

    def __init__(self, balance: float = 0.0):
        self.balance = balance
        self.transaction_count = 0
        self._lock = threading.Lock()

    def deposit(self, amount: float) -> None:
        """Безпечне поповнення рахунку з використанням контекстного менеджера."""
        with self._lock:
            current = self.balance
            time.sleep(random.uniform(0.0005, 0.002))
            self.balance = current + amount
            cnt = self.transaction_count
            time.sleep(random.uniform(0.0001, 0.0005))
            self.transaction_count = cnt + 1

    def withdraw(self, amount: float) -> bool:
        """Безпечне зняття коштів з використанням контекстного менеджера."""
        with self._lock:
            current = self.balance
            time.sleep(random.uniform(0.0005, 0.002))
            if current >= amount:
                self.balance = current - amount
                cnt = self.transaction_count
                time.sleep(random.uniform(0.0001, 0.0005))
                self.transaction_count = cnt + 1
                return True
            return False


def run_transactions(account, total_deposits: int, deposit_amount: float,
                     total_withdrawals: int, withdraw_amount: float,
                     num_threads: int = 10):
    """Запускає потоки для виконання транзакцій."""
    threads = []
    deposits_per_thread = total_deposits // (num_threads // 2)
    withdraws_per_thread = total_withdrawals // (num_threads // 2)

    def deposit_worker():
        for _ in range(deposits_per_thread):
            account.deposit(deposit_amount)

    def withdraw_worker():
        for _ in range(withdraws_per_thread):
            account.withdraw(withdraw_amount)

    # Створюємо 5 потоків для депозиту та 5 для зняття (разом 10 потоків)
    for _ in range(num_threads // 2):
        t_dep = threading.Thread(target=deposit_worker)
        t_wit = threading.Thread(target=withdraw_worker)
        threads.extend([t_dep, t_wit])

    # Запускаємо всі потоки
    for t in threads:
        t.start()

    # Очікуємо завершення роботи всіх потоків
    for t in threads:
        t.join()


def main():
    initial_balance = 1000.0
    num_deposits = 100
    deposit_amount = 50.0
    num_withdrawals = 100
    withdraw_amount = 50.0
    num_threads = 10  # 5 потоків депозитів + 5 потоків зняттів

    expected_balance = (
        initial_balance
        + num_deposits * deposit_amount
        - num_withdrawals * withdraw_amount
    )
    expected_transactions = num_deposits + num_withdrawals

    print("=" * 60)
    print("=== Завдання 2.1: Гонка даних та захист через Lock ===")
    print("=" * 60)
    print(f"Початковий баланс: {initial_balance:.2f} грн")
    print(f"Кількість операцій поповнення: {num_deposits} по {deposit_amount:.2f} грн")
    print(f"Кількість операцій зняття:     {num_withdrawals} по {withdraw_amount:.2f} грн")
    print(f"Очікуваний баланс:             {expected_balance:.2f} грн")
    print(f"Очікувана к-ть транзакцій:     {expected_transactions}")
    print(f"Кількість конкурентних потоків: {num_threads}")

    # === Тест 1: Без синхронізації ===
    print("\n" + "=" * 40)
    print("=== Тест без синхронізації (BankAccount) ===")
    print("=" * 40)
    unsafe_account = BankAccount(initial_balance)
    start_unsafe = time.perf_counter()
    run_transactions(
        unsafe_account,
        num_deposits,
        deposit_amount,
        num_withdrawals,
        withdraw_amount,
        num_threads,
    )
    time_unsafe = time.perf_counter() - start_unsafe

    is_balance_correct = abs(unsafe_account.balance - expected_balance) < 1e-6
    is_count_correct = unsafe_account.transaction_count == expected_transactions

    print(f"Фактичний баланс:         {unsafe_account.balance:.2f} грн")
    print(f"Очікуваний баланс:        {expected_balance:.2f} грн")
    print(f"Баланс збігається?        {'ТАК' if is_balance_correct else 'НІ (RACE CONDITION!)'}")
    print(f"Втрата/розбіжність:       {unsafe_account.balance - expected_balance:+.2f} грн")
    print(f"Зафіксовано транзакцій:   {unsafe_account.transaction_count} з {expected_transactions}")
    print(f"К-ть транзакцій збіглась? {'ТАК' if is_count_correct else 'НІ (ВТРАЧЕНІ ОНОВЛЕННЯ!)'}")
    print(f"Час виконання:            {time_unsafe:.3f} сек")

    # === Тест 2: З синхронізацією (Lock) ===
    print("\n" + "=" * 40)
    print("=== Тест із синхронізацією (SafeBankAccount - Lock) ===")
    print("=" * 40)
    safe_account = SafeBankAccount(initial_balance)
    start_safe = time.perf_counter()
    run_transactions(
        safe_account,
        num_deposits,
        deposit_amount,
        num_withdrawals,
        withdraw_amount,
        num_threads,
    )
    time_safe = time.perf_counter() - start_safe

    is_safe_balance_correct = abs(safe_account.balance - expected_balance) < 1e-6
    is_safe_count_correct = safe_account.transaction_count == expected_transactions

    print(f"Фактичний баланс:         {safe_account.balance:.2f} грн")
    print(f"Очікуваний баланс:        {expected_balance:.2f} грн")
    print(f"Баланс збігається?        {'ТАК (100% КОРЕКТНО)' if is_safe_balance_correct else 'НІ'}")
    print(f"Зафіксовано транзакцій:   {safe_account.transaction_count} з {expected_transactions}")
    print(f"К-ть транзакцій збіглась? {'ТАК (100% КОРЕКТНО)' if is_safe_count_correct else 'НІ'}")
    print(f"Час виконання:            {time_safe:.3f} сек")
    print("=" * 60)


if __name__ == "__main__":
    main()
