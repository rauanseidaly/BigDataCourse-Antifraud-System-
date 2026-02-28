"""
FastAPI Anti-Fraud CRM Backend
"""
import os
import sys
import math
import tempfile
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# ── Пути ─────────────────────────────────────────────────────────
# Структура: antifraud/backend/main.py → antifraud/frontend/
BASE_DIR     = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")
INDEX_HTML   = os.path.join(FRONTEND_DIR, "index.html")

sys.path.insert(0, os.path.join(BASE_DIR, "backend"))
sys.path.insert(0, BASE_DIR)

from db.database import get_connection, init_db
from etl.pipeline import ETLPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"BASE_DIR     : {BASE_DIR}")
logger.info(f"FRONTEND_DIR : {FRONTEND_DIR}  exists={os.path.exists(FRONTEND_DIR)}")
logger.info(f"INDEX_HTML   : {INDEX_HTML}  exists={os.path.exists(INDEX_HTML)}")

# ── Lifespan ──────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info("AntiFraud CRM запущен")
    yield

app = FastAPI(
    title="AntiFraud CRM API",
    description="Система обнаружения мошеннических транзакций",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Static files ──────────────────────────────────────────────────
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
    logger.info("Static files смонтированы")
else:
    logger.error(f"ПАПКА FRONTEND НЕ НАЙДЕНА: {FRONTEND_DIR}")

@app.get("/")
async def root():
    if os.path.exists(INDEX_HTML):
        return FileResponse(INDEX_HTML)
    return {"error": "index.html не найден", "ищу по пути": INDEX_HTML, "base_dir": BASE_DIR}

# ── ETL Upload ────────────────────────────────────────────────────
@app.post("/api/upload")
async def upload_transactions(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(400, "Только CSV файлы поддерживаются")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="wb") as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        pipeline = ETLPipeline()
        conn = get_connection()
        result = pipeline.run(tmp_path, conn)
        conn.close()
        return result
    except Exception as e:
        raise HTTPException(500, f"Ошибка ETL: {str(e)}")
    finally:
        os.unlink(tmp_path)

# ── Transactions ──────────────────────────────────────────────────
@app.get("/api/transactions")
async def get_transactions(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    transaction_id: Optional[str] = None,
    client_id: Optional[str] = None,
    bank: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    fraud_only: bool = False,
    sort_by: Optional[str] = "transaction_date",
    sort_dir: Optional[str] = "desc",
):
    ALLOWED_SORT = {
        "transaction_id", "client_id", "bank", "amount_kzt", "category",
        "city", "device_type", "transaction_date", "status", "is_fraud", "fraud_reason"
    }
    if sort_by not in ALLOWED_SORT:
        sort_by = "transaction_date"
    sort_dir_sql = "ASC" if sort_dir and sort_dir.lower() == "asc" else "DESC"

    conn = get_connection()
    cursor = conn.cursor()

    where_clauses = []
    params = []

    if fraud_only:
        where_clauses.append("is_fraud = 1")
    if transaction_id:
        where_clauses.append("transaction_id LIKE ?")
        params.append(f"%{transaction_id}%")
    if client_id:
        where_clauses.append("client_id LIKE ?")
        params.append(f"%{client_id}%")
    if bank:
        where_clauses.append("bank = ?")
        params.append(bank)
    if status:
        where_clauses.append("status = ?")
        params.append(status)
    if date_from:
        where_clauses.append("transaction_date >= ?")
        params.append(date_from)
    if date_to:
        where_clauses.append("transaction_date <= ?")
        params.append(date_to + " 23:59:59")

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    total = cursor.execute(f"SELECT COUNT(*) FROM transactions {where_sql}", params).fetchone()[0]
    offset = (page - 1) * per_page
    rows = cursor.execute(
        f"SELECT * FROM transactions {where_sql} ORDER BY {sort_by} {sort_dir_sql} LIMIT ? OFFSET ?",
        params + [per_page, offset]
    ).fetchall()
    conn.close()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": math.ceil(total / per_page) if total else 1,
        "data": [dict(row) for row in rows]
    }

@app.get("/api/transactions/{transaction_id}")
async def get_transaction(transaction_id: str):
    conn = get_connection()
    row = conn.execute("SELECT * FROM transactions WHERE transaction_id = ?", (transaction_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Транзакция не найдена")
    return dict(row)

# ── Dashboard ─────────────────────────────────────────────────────
@app.get("/api/dashboard/stats")
async def get_dashboard_stats():
    conn = get_connection()
    c = conn.cursor()
    total        = c.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    fraud        = c.execute("SELECT COUNT(*) FROM transactions WHERE is_fraud=1").fetchone()[0]
    total_amount = c.execute("SELECT COALESCE(SUM(amount_kzt),0) FROM transactions").fetchone()[0]
    fraud_amount = c.execute("SELECT COALESCE(SUM(amount_kzt),0) FROM transactions WHERE is_fraud=1").fetchone()[0]
    clients      = c.execute("SELECT COUNT(DISTINCT client_id) FROM transactions").fetchone()[0]
    recent       = c.execute("SELECT COUNT(*) FROM transactions WHERE transaction_date >= datetime('now', '-30 days')").fetchone()[0]
    conn.close()
    return {
        "total_transactions": total,
        "fraud_transactions": fraud,
        "legitimate_transactions": total - fraud,
        "fraud_rate": round(fraud / total * 100, 2) if total else 0,
        "total_amount_kzt": total_amount,
        "fraud_amount_kzt": fraud_amount,
        "unique_clients": clients,
        "recent_30d": recent
    }

@app.get("/api/dashboard/by_date")
async def get_by_date(days: int = Query(90, ge=7, le=365)):
    conn = get_connection()
    rows = conn.execute(f"""
        SELECT DATE(transaction_date) as date,
               COUNT(*) as total,
               SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END) as fraud,
               ROUND(SUM(amount_kzt),2) as amount
        FROM transactions
        WHERE transaction_date >= datetime('now', '-{days} days')
        GROUP BY DATE(transaction_date) ORDER BY date
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/dashboard/by_bank")
async def get_by_bank():
    conn = get_connection()
    rows = conn.execute("""
        SELECT bank, COUNT(*) as total,
               SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END) as fraud,
               ROUND(AVG(amount_kzt),2) as avg_amount,
               ROUND(SUM(amount_kzt),2) as total_amount
        FROM transactions GROUP BY bank ORDER BY total DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/dashboard/by_category")
async def get_by_category():
    conn = get_connection()
    rows = conn.execute("""
        SELECT category, COUNT(*) as total,
               SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END) as fraud,
               ROUND(SUM(amount_kzt),2) as total_amount
        FROM transactions GROUP BY category ORDER BY total DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/dashboard/by_city")
async def get_by_city():
    conn = get_connection()
    rows = conn.execute("""
        SELECT city, COUNT(*) as total,
               SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END) as fraud
        FROM transactions
        WHERE city IS NOT NULL AND city != '' AND city != 'None'
        GROUP BY city ORDER BY total DESC LIMIT 15
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/dashboard/by_hour")
async def get_by_hour():
    conn = get_connection()
    rows = conn.execute("""
        SELECT CAST(strftime('%H', transaction_date) AS INTEGER) as hour,
               COUNT(*) as total,
               SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END) as fraud
        FROM transactions GROUP BY hour ORDER BY hour
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/dashboard/fraud_reasons")
async def get_fraud_reasons():
    conn = get_connection()
    rows = conn.execute("""
        SELECT COALESCE(NULLIF(fraud_reason,''), 'unknown') as reason,
               COUNT(*) as count
        FROM transactions WHERE is_fraud=1
        GROUP BY reason ORDER BY count DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/dashboard/by_device")
async def get_by_device():
    conn = get_connection()
    rows = conn.execute("""
        SELECT device_type, COUNT(*) as total,
               SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END) as fraud
        FROM transactions
        WHERE device_type IS NOT NULL AND device_type != 'None'
        GROUP BY device_type ORDER BY total DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ── EDA ───────────────────────────────────────────────────────────
@app.get("/api/eda/summary")
async def get_eda_summary():
    conn = get_connection()
    c = conn.cursor()
    amount_stats = c.execute("""
        SELECT ROUND(MIN(amount_kzt),2) as min, ROUND(MAX(amount_kzt),2) as max,
               ROUND(AVG(amount_kzt),2) as mean, COUNT(*) as count
        FROM transactions
    """).fetchone()
    amounts = [r[0] for r in c.execute("SELECT amount_kzt FROM transactions ORDER BY amount_kzt").fetchall()]

    def pct(data, p):
        if not data: return 0
        return round(data[min(int(len(data) * p / 100), len(data)-1)], 2)

    bank_fraud = c.execute("""
        SELECT bank,
               ROUND(AVG(CAST(is_fraud AS FLOAT)) * 100, 2) as fraud_rate,
               ROUND(AVG(amount_kzt),2) as avg_amount,
               COUNT(*) as total
        FROM transactions GROUP BY bank ORDER BY fraud_rate DESC
    """).fetchall()

    top_clients = c.execute("""
        SELECT client_id, COUNT(*) as tx_count,
               SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END) as fraud_count,
               ROUND(SUM(amount_kzt),2) as total_amount
        FROM transactions GROUP BY client_id ORDER BY tx_count DESC LIMIT 10
    """).fetchall()
    conn.close()

    return {
        "amount_stats": {
            "min": amount_stats["min"], "max": amount_stats["max"],
            "mean": amount_stats["mean"], "count": amount_stats["count"],
            "p25": pct(amounts, 25), "p50": pct(amounts, 50),
            "p75": pct(amounts, 75), "p95": pct(amounts, 95), "p99": pct(amounts, 99),
        },
        "bank_fraud_rates": [dict(r) for r in bank_fraud],
        "top_clients": [dict(r) for r in top_clients],
    }

@app.get("/api/eda/amount_distribution")
async def get_amount_distribution():
    conn = get_connection()
    buckets = [
        (0, 5000, "0–5к"), (5000, 20000, "5к–20к"), (20000, 50000, "20к–50к"),
        (50000, 100000, "50к–100к"), (100000, 500000, "100к–500к"),
        (500000, 1000000, "500к–1М"), (1000000, 999999999, ">1М"),
    ]
    result = []
    for low, high, label in buckets:
        row = conn.execute("""
            SELECT COUNT(*) as count, SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END) as fraud
            FROM transactions WHERE amount_kzt >= ? AND amount_kzt < ?
        """, (low, high)).fetchone()
        result.append({"bucket": label, "count": row["count"], "fraud": row["fraud"]})
    conn.close()
    return result

# ── Misc ──────────────────────────────────────────────────────────
@app.get("/api/banks")
async def get_banks():
    conn = get_connection()
    rows = conn.execute("SELECT DISTINCT bank FROM transactions ORDER BY bank").fetchall()
    conn.close()
    return [r["bank"] for r in rows]

@app.get("/api/etl/logs")
async def get_etl_logs():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM etl_logs ORDER BY started_at DESC LIMIT 20").fetchall()
    conn.close()
    return [dict(r) for r in rows]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)