"""
Генератор синтетических транзакций казахстанских банков
"""
import csv
import random
import uuid
from datetime import datetime, timedelta

# Казахстанские банки
BANKS = [
    "Kaspi Bank", "Halyk Bank", "Jusan Bank", "ForteBank",
    "Bank CenterCredit", "ATF Bank", "Евразийский Банк", "Нурбанк",
    "RBK Bank", "Сбербанк Казахстан"
]

CITIES = [
    "Алматы", "Астана", "Шымкент", "Актобе", "Тараз",
    "Павлодар", "Усть-Каменогорск", "Семей", "Атырау", "Костанай"
]

CATEGORIES = [
    "Перевод", "Оплата товаров", "Коммунальные услуги",
    "Ресторан", "Транспорт", "Медицина", "Образование",
    "Развлечения", "Онлайн-покупки", "Снятие наличных"
]

FRAUD_PATTERNS = [
    "multiple_small_transactions",
    "unusual_location",
    "high_amount_unusual_time",
    "rapid_transactions",
    "international_transfer"
]

def generate_iin():
    """Генерация ИИН (Индивидуальный идентификационный номер)"""
    year = random.randint(60, 99)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    gender = random.choice([3, 4])  # 3-мужчина, 4-женщина рожд. в 1900-1999
    rest = random.randint(10000, 99999)
    return f"{year:02d}{month:02d}{day:02d}{gender}{rest}"

def generate_transactions(n=5000):
    transactions = []
    start_date = datetime(2023, 1, 1)
    
    clients = [generate_iin() for _ in range(500)]
    
    for i in range(n):
        client_id = random.choice(clients)
        bank = random.choice(BANKS)
        city = random.choice(CITIES)
        category = random.choice(CATEGORIES)
        
        # Дата транзакции
        days_offset = random.randint(0, 730)
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        tx_date = start_date + timedelta(days=days_offset, hours=hours, minutes=minutes)
        
        # Сумма в тенге
        if category == "Снятие наличных":
            amount = random.choice([
                random.randint(5000, 50000),
                random.randint(50000, 500000)
            ])
        elif category in ["Коммунальные услуги", "Транспорт"]:
            amount = random.randint(500, 15000)
        elif category == "Онлайн-покупки":
            amount = random.randint(2000, 300000)
        else:
            amount = random.randint(1000, 200000)
        
        # Определение мошенничества (примерно 8% транзакций)
        is_fraud = 0
        fraud_reason = ""
        
        rand = random.random()
        if rand < 0.03:
            # Очень большая сумма ночью
            if hours in range(1, 5):
                amount = random.randint(500000, 5000000)
                is_fraud = 1
                fraud_reason = "high_amount_unusual_time"
        elif rand < 0.05:
            # Международный перевод на крупную сумму
            amount = random.randint(1000000, 10000000)
            is_fraud = 1
            fraud_reason = "international_transfer"
        elif rand < 0.07:
            # Подозрительно много мелких транзакций
            amount = random.randint(100, 999)
            is_fraud = 1
            fraud_reason = "multiple_small_transactions"
        elif rand < 0.08:
            is_fraud = 1
            fraud_reason = random.choice(FRAUD_PATTERNS)
        
        # Статус
        statuses = ["completed", "completed", "completed", "completed", "pending", "failed"]
        status = random.choice(statuses)
        if is_fraud and random.random() > 0.3:
            status = "completed"
        
        # Тип устройства
        device = random.choice(["mobile_app", "web", "pos_terminal", "atm", "ussd"])
        
        tx = {
            "transaction_id": str(uuid.uuid4()),
            "client_id": client_id,
            "bank": bank,
            "sender_account": f"KZ{random.randint(10,99)}{random.randint(1000000000000000, 9999999999999999)}",
            "receiver_account": f"KZ{random.randint(10,99)}{random.randint(1000000000000000, 9999999999999999)}",
            "amount_kzt": amount,
            "category": category,
            "city": city,
            "device_type": device,
            "transaction_date": tx_date.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "is_fraud": is_fraud,
            "fraud_reason": fraud_reason,
            "description": f"{category} - {city}"
        }
        transactions.append(tx)
    
    return transactions

if __name__ == "__main__":
    print("Генерация синтетических транзакций...")
    transactions = generate_transactions(5000)
    
    output_path = "/home/claude/antifrod/data/transactions_kz.csv"
    
    fieldnames = list(transactions[0].keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)
    
    fraud_count = sum(1 for t in transactions if t["is_fraud"] == 1)
    print(f"✅ Создано {len(transactions)} транзакций")
    print(f"🚨 Из них мошеннических: {fraud_count} ({fraud_count/len(transactions)*100:.1f}%)")
    print(f"📁 Файл: {output_path}")