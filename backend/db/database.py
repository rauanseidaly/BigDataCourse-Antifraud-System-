"""
Database models using SQLite
"""
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "antifrod.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT UNIQUE NOT NULL,
    client_id TEXT NOT NULL,
    bank TEXT NOT NULL,
    sender_account TEXT,
    receiver_account TEXT,
    amount_kzt REAL NOT NULL,
    category TEXT,
    city TEXT,
    device_type TEXT,
    transaction_date TEXT NOT NULL,
    status TEXT DEFAULT 'completed',
    is_fraud INTEGER DEFAULT 0,
    fraud_reason TEXT,
    fraud_score REAL DEFAULT 0.0,
    description TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    total_rows INTEGER,
    valid_rows INTEGER,
    invalid_rows INTEGER,
    fraud_detected INTEGER,
    status TEXT,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tx_client ON transactions(client_id);
CREATE INDEX IF NOT EXISTS idx_tx_bank ON transactions(bank);
CREATE INDEX IF NOT EXISTS idx_tx_fraud ON transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(transaction_date);
"""

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_connection()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print(f"✅ База данных инициализирована: {DB_PATH}")

if __name__ == "__main__":
    init_db()