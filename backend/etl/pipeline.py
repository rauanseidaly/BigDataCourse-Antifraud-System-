"""
ETL Pipeline для обработки транзакций с использованием PySpark
"""
import os
import sys
import logging
from datetime import datetime
from typing import Tuple, Dict, Any

logger = logging.getLogger(__name__)

# Попытка импорта PySpark, fallback на pandas если не установлен
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        IntegerType, TimestampType
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logger.warning("PySpark не установлен. Используется pandas fallback.")

import pandas as pd
import numpy as np


TRANSACTION_SCHEMA_SPARK = None
if SPARK_AVAILABLE:
    TRANSACTION_SCHEMA_SPARK = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("bank", StringType(), True),
        StructField("sender_account", StringType(), True),
        StructField("receiver_account", StringType(), True),
        StructField("amount_kzt", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("city", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("is_fraud", IntegerType(), True),
        StructField("fraud_reason", StringType(), True),
        StructField("description", StringType(), True),
    ])

VALID_BANKS = [
    "Kaspi Bank", "Halyk Bank", "Jusan Bank", "ForteBank",
    "Bank CenterCredit", "ATF Bank", "Евразийский Банк", "Нурбанк",
    "RBK Bank", "Сбербанк Казахстан"
]

VALID_STATUSES = ["completed", "pending", "failed", "cancelled"]
VALID_CATEGORIES = [
    "Перевод", "Оплата товаров", "Коммунальные услуги",
    "Ресторан", "Транспорт", "Медицина", "Образование",
    "Развлечения", "Онлайн-покупки", "Снятие наличных"
]


class ETLPipeline:
    """ETL Pipeline для обработки CSV файлов с транзакциями"""
    
    def __init__(self):
        self.spark = None
        if SPARK_AVAILABLE:
            try:
                self.spark = SparkSession.builder \
                    .appName("AntiFraudETL") \
                    .config("spark.sql.shuffle.partitions", "4") \
                    .config("spark.driver.memory", "512m") \
                    .master("local[*]") \
                    .getOrCreate()
                self.spark.sparkContext.setLogLevel("ERROR")
                logger.info("✅ Spark сессия запущена")
            except Exception as e:
                logger.warning(f"Не удалось запустить Spark: {e}. Используется pandas.")
                self.spark = None
    
    def extract(self, filepath: str) -> Tuple[pd.DataFrame, Dict]:
        """
        EXTRACT: Чтение CSV файла
        """
        stats = {"total_rows": 0, "extract_errors": []}
        
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
            stats["total_rows"] = len(df)
            logger.info(f"📥 Extracted {len(df)} rows from {filepath}")
            return df, stats
        except UnicodeDecodeError:
            df = pd.read_csv(filepath, encoding='cp1251')
            stats["total_rows"] = len(df)
            return df, stats
        except Exception as e:
            stats["extract_errors"].append(str(e))
            raise ValueError(f"Ошибка чтения файла: {e}")
    
    def transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        TRANSFORM: Очистка и трансформация данных
        """
        stats = {
            "original_rows": len(df),
            "dropped_duplicates": 0,
            "dropped_nulls": 0,
            "dropped_invalid": 0,
            "amount_outliers_capped": 0,
            "dates_fixed": 0
        }
        
        # ── 1. Нормализация имен колонок ──────────────────────────────
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        
        # ── 2. Удаление дубликатов по transaction_id ──────────────────
        if "transaction_id" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["transaction_id"])
            stats["dropped_duplicates"] = before - len(df)
        
        # ── 3. Проверка обязательных полей ────────────────────────────
        required_cols = ["transaction_id", "client_id", "bank", "amount_kzt", "transaction_date"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Отсутствуют обязательные колонки: {missing_cols}")
        
        before = len(df)
        df = df.dropna(subset=["transaction_id", "client_id", "amount_kzt"])
        stats["dropped_nulls"] = before - len(df)
        
        # Ensure string types for key fields
        df["transaction_id"] = df["transaction_id"].astype(str)
        df["client_id"] = df["client_id"].astype(str)
        
        # ── 4. Очистка числовых полей ─────────────────────────────────
        df["amount_kzt"] = pd.to_numeric(df["amount_kzt"], errors="coerce")
        before = len(df)
        df = df[df["amount_kzt"] > 0]
        df = df[df["amount_kzt"] <= 50_000_000]  # Макс. 50 млн тенге
        stats["dropped_invalid"] += before - len(df)
        
        # Кэппинг аномальных значений (IQR)
        q99 = df["amount_kzt"].quantile(0.99)
        outliers = df["amount_kzt"] > q99 * 3
        stats["amount_outliers_capped"] = outliers.sum()
        
        # ── 5. Парсинг и валидация дат ────────────────────────────────
        df["transaction_date"] = pd.to_datetime(
            df["transaction_date"], 
            format="%Y-%m-%d %H:%M:%S", 
            errors="coerce"
        )
        
        before = len(df)
        df = df.dropna(subset=["transaction_date"])
        stats["dates_fixed"] = before - len(df)
        
        # Убираем будущие даты
        df = df[df["transaction_date"] <= pd.Timestamp.now()]
        
        # ── 6. Очистка строковых полей ────────────────────────────────
        str_cols = ["bank", "category", "city", "device_type", "status", "description"]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace("nan", None)
        
        # ── 7. Стандартизация банков ──────────────────────────────────
        if "bank" in df.columns:
            df["bank"] = df["bank"].fillna("Неизвестный банк")
        
        # ── 8. Нормализация is_fraud ──────────────────────────────────
        if "is_fraud" in df.columns:
            df["is_fraud"] = pd.to_numeric(df["is_fraud"], errors="coerce").fillna(0).astype(int)
            df["is_fraud"] = df["is_fraud"].clip(0, 1)
        else:
            df["is_fraud"] = 0
        
        # ── 9. Заполнение пустых полей ────────────────────────────────
        df["fraud_reason"] = df.get("fraud_reason", pd.Series([""] * len(df))).fillna("")
        df["status"] = df.get("status", pd.Series(["completed"] * len(df))).fillna("completed")
        df["category"] = df.get("category", pd.Series(["Прочее"] * len(df))).fillna("Прочее")
        df["description"] = df.get("description", pd.Series([""] * len(df))).fillna("")
        
        # ── 10. Добавление вычисляемых полей ─────────────────────────
        df["fraud_score"] = df["is_fraud"].astype(float)
        
        # Конвертируем дату обратно в строку для SQLite
        df["transaction_date"] = df["transaction_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
        
        stats["valid_rows"] = len(df)
        logger.info(f"✅ Transform: {stats['original_rows']} → {stats['valid_rows']} rows")
        logger.info(f"   Дубликаты удалены: {stats['dropped_duplicates']}")
        logger.info(f"   Нулевые значения: {stats['dropped_nulls']}")
        logger.info(f"   Невалидные записи: {stats['dropped_invalid']}")
        
        return df, stats
    
    def transform_with_spark(self, filepath: str) -> Tuple[pd.DataFrame, Dict]:
        """
        TRANSFORM с использованием PySpark (если доступен)
        """
        stats = {}
        
        df_spark = self.spark.read.csv(
            filepath,
            header=True,
            inferSchema=False,
            schema=TRANSACTION_SCHEMA_SPARK
        )
        
        total = df_spark.count()
        stats["total_rows"] = total
        
        # Дедупликация
        df_spark = df_spark.dropDuplicates(["transaction_id"])
        
        # Удаление null в обязательных полях
        df_spark = df_spark.dropna(subset=["transaction_id", "client_id", "amount_kzt"])
        
        # Фильтр суммы
        df_spark = df_spark.filter(
            (F.col("amount_kzt") > 0) & (F.col("amount_kzt") <= 50_000_000)
        )
        
        # Парсинг даты
        df_spark = df_spark.withColumn(
            "transaction_date",
            F.to_timestamp("transaction_date", "yyyy-MM-dd HH:mm:ss")
        )
        df_spark = df_spark.dropna(subset=["transaction_date"])
        df_spark = df_spark.filter(F.col("transaction_date") <= F.current_timestamp())
        
        # Форматируем дату обратно
        df_spark = df_spark.withColumn(
            "transaction_date",
            F.date_format("transaction_date", "yyyy-MM-dd HH:mm:ss")
        )
        
        # Нормализация
        df_spark = df_spark.withColumn("is_fraud", F.coalesce(F.col("is_fraud"), F.lit(0)))
        df_spark = df_spark.withColumn("fraud_score", F.col("is_fraud").cast(DoubleType()))
        df_spark = df_spark.fillna({
            "fraud_reason": "",
            "status": "completed",
            "category": "Прочее",
            "description": ""
        })
        
        valid_rows = df_spark.count()
        stats["valid_rows"] = valid_rows
        stats["dropped_duplicates"] = total - valid_rows
        
        # Конвертация в pandas для загрузки в SQLite
        df_pandas = df_spark.toPandas()
        
        return df_pandas, stats
    
    def load(self, df: pd.DataFrame, db_conn, etl_log_id: int) -> Dict:
        """
        LOAD: Загрузка данных в SQLite
        """
        stats = {"inserted": 0, "skipped": 0, "errors": []}
        
        cursor = db_conn.cursor()
        
        # Загружаем клиентов
        clients = df["client_id"].unique()
        for client_id in clients:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO clients (client_id) VALUES (?)",
                    (str(client_id),)
                )
            except Exception:
                pass
        
        # Загружаем транзакции
        columns = [
            "transaction_id", "client_id", "bank", "sender_account",
            "receiver_account", "amount_kzt", "category", "city",
            "device_type", "transaction_date", "status", "is_fraud",
            "fraud_reason", "fraud_score", "description"
        ]
        
        # Убеждаемся что все нужные колонки есть
        for col in columns:
            if col not in df.columns:
                df[col] = None
        
        insert_sql = f"""
            INSERT OR IGNORE INTO transactions 
            ({', '.join(columns)})
            VALUES ({', '.join(['?' for _ in columns])})
        """
        
        for _, row in df.iterrows():
            try:
                def to_py(val):
                    if pd.isna(val) if not isinstance(val, str) else False:
                        return None
                    # Convert numpy types to native Python
                    if hasattr(val, 'item'):
                        return val.item()
                    return val

                values = tuple(to_py(row.get(col)) for col in columns)
                cursor.execute(insert_sql, values)
                stats["inserted"] += 1
            except Exception as e:
                stats["skipped"] += 1
                if len(stats["errors"]) < 10:
                    stats["errors"].append(str(e))
        
        db_conn.commit()
        logger.info(f"💾 Loaded: {stats['inserted']} inserted, {stats['skipped']} skipped")
        return stats
    
    def run(self, filepath: str, db_conn) -> Dict:
        """
        Запуск полного ETL пайплайна
        """
        started_at = datetime.now()
        result = {
            "status": "success",
            "filename": os.path.basename(filepath),
            "started_at": started_at.isoformat(),
        }
        
        cursor = db_conn.cursor()
        cursor.execute(
            "INSERT INTO etl_logs (filename, status, started_at) VALUES (?, 'running', ?)",
            (os.path.basename(filepath), started_at)
        )
        log_id = cursor.lastrowid
        db_conn.commit()
        
        try:
            # EXTRACT
            if self.spark and SPARK_AVAILABLE:
                try:
                    df, transform_stats = self.transform_with_spark(filepath)
                    extract_stats = {"total_rows": transform_stats.get("total_rows", len(df))}
                except Exception as e:
                    logger.warning(f"Spark failed: {e}, fallback to pandas")
                    df, extract_stats = self.extract(filepath)
                    df, transform_stats = self.transform(df)
            else:
                df, extract_stats = self.extract(filepath)
                df, transform_stats = self.transform(df)
            
            # LOAD
            load_stats = self.load(df, db_conn, log_id)
            
            # Статистика
            total_rows = extract_stats.get("total_rows", 0)
            valid_rows = transform_stats.get("valid_rows", len(df))
            fraud_count = int(df["is_fraud"].sum()) if "is_fraud" in df.columns else 0
            
            cursor.execute("""
                UPDATE etl_logs SET
                    total_rows=?, valid_rows=?, invalid_rows=?,
                    fraud_detected=?, status='success', finished_at=?
                WHERE id=?
            """, (
                total_rows, valid_rows,
                total_rows - valid_rows,
                fraud_count,
                datetime.now(), log_id
            ))
            db_conn.commit()
            
            result.update({
                "log_id": log_id,
                "total_rows": total_rows,
                "valid_rows": valid_rows,
                "invalid_rows": total_rows - valid_rows,
                "fraud_detected": fraud_count,
                "inserted": load_stats["inserted"],
                "skipped": load_stats["skipped"],
                "finished_at": datetime.now().isoformat()
            })
            
        except Exception as e:
            cursor.execute(
                "UPDATE etl_logs SET status='error', error_message=?, finished_at=? WHERE id=?",
                (str(e), datetime.now(), log_id)
            )
            db_conn.commit()
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"ETL Error: {e}")
        
        return result