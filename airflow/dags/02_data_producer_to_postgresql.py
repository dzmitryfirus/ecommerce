from datetime import datetime, timezone, timedelta
import logging
import uuid
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict
from pymongo import MongoClient
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from airflow.models import Variable

logger = logging.getLogger(__name__)

ECOMMERCE_DATASET = Dataset("postgres://warehouse/dds_ecommerce_data")


# ============================================================
# Pydantic модели
# ============================================================

class UserModel(BaseModel):
    user_id: str = Field(..., description="UUID пользователя")
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    email: str = Field(..., description="Email")
    phone: str = Field(..., description="Телефон")
    country: str = Field(..., min_length=2)
    age: int = Field(..., ge=18, le=120)
    registration_date: datetime = Field(...)
    is_active: bool = Field(...)
    lifetime_value: float = Field(..., ge=0)
    created_at: datetime = Field(...)
    
    @field_validator('email')
    @classmethod
    def validate_email(cls, v: str) -> str:
        if '@' not in v or '.' not in v:
            raise ValueError(f'Invalid email format: {v}')
        return v
    
    @field_validator('phone')
    @classmethod
    def validate_phone(cls, v: str) -> str:
        if not v.startswith('+') or len(v) < 7:
            raise ValueError(f'Invalid phone format: {v}')
        return v


class ProductModel(BaseModel):
    product_id: str = Field(..., pattern=r'^PROD_[A-Z0-9]{8}$')
    name: str = Field(..., min_length=1, max_length=255)
    category: str = Field(...)
    price: float = Field(..., gt=0, le=100000)
    stock: int = Field(..., ge=0)
    rating: float = Field(..., ge=0, le=5)
    created_at: datetime = Field(...)


class OrderItemModel(BaseModel):
    product_id: str
    quantity: int = Field(..., ge=1, le=100)
    price: float = Field(..., gt=0)
    total: float = Field(..., gt=0)


class OrderModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    order_id: str = Field(..., pattern=r'^ORD_[A-Z0-9]{12}$')
    user_id: str
    items: List[OrderItemModel]
    total_amount: float = Field(..., gt=0)
    status: str
    payment_method: str
    created_at: datetime
    
    @field_validator('status')
    @classmethod
    def validate_status(cls, v: str) -> str:
        allowed = {"pending", "processing", "shipped", "delivered", "cancelled", "refunded"}
        if v not in allowed:
            raise ValueError(f'status must be one of {allowed}, got: {v}')
        return v
    
    @field_validator('payment_method')
    @classmethod
    def validate_payment_method(cls, v: str) -> str:
        allowed = {"card", "paypal", "apple_pay", "google_pay", "crypto"}
        if v not in allowed:
            raise ValueError(f'payment_method must be one of {allowed}, got: {v}')
        return v
    
    @field_validator('total_amount')
    @classmethod
    def validate_total_amount(cls, v: float, info) -> float:
        items = info.data.get('items', [])
        calculated_total = sum(item.total for item in items)
        if abs(v - calculated_total) > 0.01:
            raise ValueError(f'total_amount {v} does not match sum of items {calculated_total}')
        return v


class EventModel(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    session_id: str
    timestamp: datetime
    page: str
    device: str
    browser: str
    referrer: Optional[str] = None
    
    @field_validator('event_type')
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        allowed = {"view_product", "add_to_cart", "remove_from_cart", "start_checkout", "purchase"}
        if v not in allowed:
            raise ValueError(f'event_type must be one of {allowed}, got: {v}')
        return v
    
    @field_validator('device')
    @classmethod
    def validate_device(cls, v: str) -> str:
        allowed = {"mobile", "desktop", "tablet"}
        if v not in allowed:
            raise ValueError(f'device must be one of {allowed}, got: {v}')
        return v


# ============================================================
# DAG Producer
# ============================================================

@dag(
    dag_id="02_ecommerce_producer_mongodb_to_postgres",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "producer", "mongodb", "postgres", "dds"],
    description="Читает данные из MongoDB, валидирует, загружает в Staging и DDS слои PostgreSQL"
)
def ecommerce_producer_dag():
    
    @task(outlets=[ECOMMERCE_DATASET])
    def publish_dataset():
        logger.info(f"📡 Публикуем Dataset: {ECOMMERCE_DATASET.uri}")
        return True
    
    @task
    def start_process() -> str:
        logger.info("=" * 60)
        logger.info("🚀 Начало работы E-commerce Producer DAG")
        logger.info(f"Время запуска: {datetime.now()}")
        logger.info("=" * 60)
        return "started"
    
    @task
    def create_postgres_tables() -> bool:
        """Создание Staging и DDS таблиц"""
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        create_tables_sql = """
        -- STAGING TABLES
        CREATE TABLE IF NOT EXISTS stg_ecommerce_users (
            user_id TEXT PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT NOT NULL,
            country TEXT NOT NULL,
            age INTEGER NOT NULL,
            registration_date TIMESTAMPTZ NOT NULL,
            is_active BOOLEAN NOT NULL,
            lifetime_value DECIMAL(10,2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        CREATE TABLE IF NOT EXISTS stg_ecommerce_products (
            product_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            stock INTEGER NOT NULL,
            rating DECIMAL(3,1) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        CREATE TABLE IF NOT EXISTS stg_ecommerce_orders (
            order_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            total_amount DECIMAL(10,2) NOT NULL,
            status TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        CREATE TABLE IF NOT EXISTS stg_ecommerce_order_items (
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            total DECIMAL(10,2) NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (order_id, product_id)
        );
        
        CREATE TABLE IF NOT EXISTS stg_ecommerce_events (
            event_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            session_id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            page TEXT NOT NULL,
            device TEXT NOT NULL,
            browser TEXT NOT NULL,
            referrer TEXT,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        -- DDS TABLES
        CREATE TABLE IF NOT EXISTS dds_dim_date (
            date_sk SERIAL PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name TEXT NOT NULL,
            day INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            day_name TEXT NOT NULL,
            week_of_year INTEGER NOT NULL,
            is_weekend BOOLEAN NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS dds_dim_users (
            user_sk SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT NOT NULL,
            country TEXT NOT NULL,
            age INTEGER NOT NULL,
            age_group TEXT NOT NULL,
            lifetime_value DECIMAL(10,2) NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE,
            is_current BOOLEAN DEFAULT TRUE
        );
        
        CREATE TABLE IF NOT EXISTS dds_dim_products (
            product_sk SERIAL PRIMARY KEY,
            product_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            price_category TEXT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS dds_fact_sales (
            sale_sk SERIAL PRIMARY KEY,
            date_sk INTEGER NOT NULL REFERENCES dds_dim_date(date_sk),
            user_sk INTEGER NOT NULL REFERENCES dds_dim_users(user_sk),
            product_sk INTEGER NOT NULL REFERENCES dds_dim_products(product_sk),
            order_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        CREATE TABLE IF NOT EXISTS dds_fact_events (
            event_sk SERIAL PRIMARY KEY,
            date_sk INTEGER NOT NULL REFERENCES dds_dim_date(date_sk),
            user_sk INTEGER NOT NULL REFERENCES dds_dim_users(user_sk),
            event_id TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            session_id TEXT NOT NULL,
            page TEXT NOT NULL,
            device TEXT NOT NULL,
            browser TEXT NOT NULL,
            referrer TEXT,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_dds_fact_sales_date ON dds_fact_sales(date_sk);
        CREATE INDEX IF NOT EXISTS idx_dds_fact_sales_user ON dds_fact_sales(user_sk);
        CREATE INDEX IF NOT EXISTS idx_dds_fact_sales_product ON dds_fact_sales(product_sk);
        """
        
        try:
            hook.run(create_tables_sql)
            logger.info("✅ Staging и DDS таблицы созданы/проверены")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            raise
    
    # ============================================================
    # EXTRACT
    # ============================================================
    
    @task
    def extract_users() -> List[Dict]:
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        five_min_ago = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        with MongoClient(mongo_uri) as client:
            db = client["source_db"]
            users = list(db["ecommerce_users"].find({"created_at": {"$gte": five_min_ago}}, {"_id": 0}))
            logger.info(f"✅ Извлечено пользователей: {len(users)}")
            return users
    
    @task
    def extract_products() -> List[Dict]:
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        five_min_ago = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        with MongoClient(mongo_uri) as client:
            db = client["source_db"]
            products = list(db["ecommerce_products"].find({"created_at": {"$gte": five_min_ago}}, {"_id": 0}))
            logger.info(f"✅ Извлечено товаров: {len(products)}")
            return products
    
    @task
    def extract_orders() -> List[Dict]:
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        five_min_ago = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        with MongoClient(mongo_uri) as client:
            db = client["source_db"]
            orders = list(db["ecommerce_orders"].find({"created_at": {"$gte": five_min_ago}}, {"_id": 0}))
            logger.info(f"✅ Извлечено заказов: {len(orders)}")
            return orders
    
    @task
    def extract_events() -> List[Dict]:
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        five_min_ago = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        with MongoClient(mongo_uri) as client:
            db = client["source_db"]
            events = list(db["ecommerce_events"].find({"timestamp": {"$gte": five_min_ago}}, {"_id": 0}))
            logger.info(f"✅ Извлечено событий: {len(events)}")
            return events
    
    # ============================================================
    # VALIDATION
    # ============================================================
    
    @task
    def validate_users(users: List[Dict]) -> Dict[str, Any]:
        valid_users = []
        invalid_count = 0
        invalid_by_reason = {}
        
        if not users:
            return {"total": 0, "valid": 0, "invalid": 0, "invalid_by_reason": {}, "valid_data": []}
        
        for user in users:
            try:
                validated = UserModel.model_validate(user)
                valid_users.append(validated.model_dump())
            except Exception as e:
                invalid_count += 1
                error_type = type(e).__name__
                invalid_by_reason[error_type] = invalid_by_reason.get(error_type, 0) + 1
        
        logger.info(f"📊 Пользователи: валидных {len(valid_users)}, невалидных {invalid_count}")
        return {"total": len(users), "valid": len(valid_users), "invalid": invalid_count,
                "invalid_by_reason": invalid_by_reason, "valid_data": valid_users}
    
    @task
    def validate_products(products: List[Dict]) -> Dict[str, Any]:
        valid_products = []
        invalid_count = 0
        invalid_by_reason = {}
        
        if not products:
            return {"total": 0, "valid": 0, "invalid": 0, "invalid_by_reason": {}, "valid_data": []}
        
        for product in products:
            try:
                validated = ProductModel.model_validate(product)
                valid_products.append(validated.model_dump())
            except Exception as e:
                invalid_count += 1
                error_type = type(e).__name__
                invalid_by_reason[error_type] = invalid_by_reason.get(error_type, 0) + 1
        
        logger.info(f"📊 Товары: валидных {len(valid_products)}, невалидных {invalid_count}")
        return {"total": len(products), "valid": len(valid_products), "invalid": invalid_count,
                "invalid_by_reason": invalid_by_reason, "valid_data": valid_products}
    
    @task
    def validate_orders(orders: List[Dict]) -> Dict[str, Any]:
        valid_orders = []
        invalid_count = 0
        invalid_by_reason = {}
        
        if not orders:
            return {"total": 0, "valid": 0, "invalid": 0, "invalid_by_reason": {}, "valid_data": []}
        
        for order in orders:
            try:
                validated = OrderModel.model_validate(order)
                valid_orders.append(validated.model_dump())
            except Exception as e:
                invalid_count += 1
                error_type = type(e).__name__
                invalid_by_reason[error_type] = invalid_by_reason.get(error_type, 0) + 1
        
        logger.info(f"📊 Заказы: валидных {len(valid_orders)}, невалидных {invalid_count}")
        return {"total": len(orders), "valid": len(valid_orders), "invalid": invalid_count,
                "invalid_by_reason": invalid_by_reason, "valid_data": valid_orders}
    
    @task
    def validate_events(events: List[Dict]) -> Dict[str, Any]:
        valid_events = []
        invalid_count = 0
        invalid_by_reason = {}
        
        if not events:
            return {"total": 0, "valid": 0, "invalid": 0, "invalid_by_reason": {}, "valid_data": []}
        
        for event in events:
            try:
                validated = EventModel.model_validate(event)
                valid_events.append(validated.model_dump())
            except Exception as e:
                invalid_count += 1
                error_type = type(e).__name__
                invalid_by_reason[error_type] = invalid_by_reason.get(error_type, 0) + 1
        
        logger.info(f"📊 События: валидных {len(valid_events)}, невалидных {invalid_count}")
        return {"total": len(events), "valid": len(valid_events), "invalid": invalid_count,
                "invalid_by_reason": invalid_by_reason, "valid_data": valid_events}
    
    # ============================================================
    # LOAD TO STAGING
    # ============================================================
    
    @task
    def load_users_to_staging(validation_result: Dict[str, Any]) -> int:
        valid_users = validation_result.get("valid_data", [])
        if not valid_users:
            return 0
        
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for user in valid_users:
                    cur.execute("""
                        INSERT INTO stg_ecommerce_users 
                        (user_id, first_name, last_name, email, phone, country, age, registration_date, is_active, lifetime_value, created_at)
                        VALUES (%(user_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s, %(country)s, %(age)s, 
                                %(registration_date)s, %(is_active)s, %(lifetime_value)s, %(created_at)s)
                        ON CONFLICT (user_id) DO UPDATE SET
                            first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,
                            email = EXCLUDED.email, phone = EXCLUDED.phone, country = EXCLUDED.country,
                            age = EXCLUDED.age, lifetime_value = EXCLUDED.lifetime_value,
                            is_active = EXCLUDED.is_active, loaded_at = NOW()
                    """, user)
                conn.commit()
        
        logger.info(f"💾 Загружено пользователей в staging: {len(valid_users)}")
        return len(valid_users)
    
    @task
    def load_products_to_staging(validation_result: Dict[str, Any]) -> int:
        valid_products = validation_result.get("valid_data", [])
        if not valid_products:
            return 0
        
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for product in valid_products:
                    cur.execute("""
                        INSERT INTO stg_ecommerce_products 
                        (product_id, name, category, price, stock, rating, created_at)
                        VALUES (%(product_id)s, %(name)s, %(category)s, %(price)s, %(stock)s, %(rating)s, %(created_at)s)
                        ON CONFLICT (product_id) DO UPDATE SET
                            name = EXCLUDED.name, category = EXCLUDED.category,
                            price = EXCLUDED.price, stock = EXCLUDED.stock,
                            rating = EXCLUDED.rating, loaded_at = NOW()
                    """, product)
                conn.commit()
        
        logger.info(f"💾 Загружено товаров в staging: {len(valid_products)}")
        return len(valid_products)
    
    @task
    def load_orders_to_staging(validation_result: Dict[str, Any]) -> int:
        valid_orders = validation_result.get("valid_data", [])
        if not valid_orders:
            return 0
        
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for order in valid_orders:
                    cur.execute("""
                        INSERT INTO stg_ecommerce_orders 
                        (order_id, user_id, total_amount, status, payment_method, created_at)
                        VALUES (%(order_id)s, %(user_id)s, %(total_amount)s, %(status)s, %(payment_method)s, %(created_at)s)
                        ON CONFLICT (order_id) DO UPDATE SET
                            status = EXCLUDED.status, payment_method = EXCLUDED.payment_method,
                            total_amount = EXCLUDED.total_amount, loaded_at = NOW()
                    """, order)
                    
                    for item in order.get("items", []):
                        item_data = {
                            "order_id": order["order_id"],
                            "product_id": item["product_id"],
                            "quantity": item["quantity"],
                            "price": item["price"],
                            "total": item["total"]
                        }
                        cur.execute("""
                            INSERT INTO stg_ecommerce_order_items 
                            (order_id, product_id, quantity, price, total)
                            VALUES (%(order_id)s, %(product_id)s, %(quantity)s, %(price)s, %(total)s)
                            ON CONFLICT (order_id, product_id) DO UPDATE SET
                                quantity = EXCLUDED.quantity, price = EXCLUDED.price,
                                total = EXCLUDED.total, loaded_at = NOW()
                        """, item_data)
                conn.commit()
        
        logger.info(f"💾 Загружено заказов в staging: {len(valid_orders)}")
        return len(valid_orders)
    
    @task
    def load_events_to_staging(validation_result: Dict[str, Any]) -> int:
        valid_events = validation_result.get("valid_data", [])
        if not valid_events:
            return 0
        
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for event in valid_events:
                    cur.execute("""
                        INSERT INTO stg_ecommerce_events 
                        (event_id, user_id, event_type, session_id, timestamp, page, device, browser, referrer)
                        VALUES (%(event_id)s, %(user_id)s, %(event_type)s, %(session_id)s, 
                                %(timestamp)s, %(page)s, %(device)s, %(browser)s, %(referrer)s)
                        ON CONFLICT (event_id) DO NOTHING
                    """, event)
                conn.commit()
        
        logger.info(f"💾 Загружено событий в staging: {len(valid_events)}")
        return len(valid_events)
    
    # ============================================================
    # TRANSFORM TO DDS (ИСПРАВЛЕНО)
    # ============================================================
    
    @task
    def transform_date_dimension():
        """Заполнение измерения Дата"""
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        hook.run("""
            INSERT INTO dds_dim_date (date, year, quarter, month, month_name, day, day_of_week, day_name, week_of_year, is_weekend)
            WITH date_range AS (
                SELECT generate_series(
                    DATE_TRUNC('year', NOW()) - INTERVAL '1 year',
                    DATE_TRUNC('year', NOW()) + INTERVAL '2 years',
                    '1 day'::interval
                )::DATE as date
            )
            SELECT 
                date,
                EXTRACT(YEAR FROM date) as year,
                EXTRACT(QUARTER FROM date) as quarter,
                EXTRACT(MONTH FROM date) as month,
                TO_CHAR(date, 'Month') as month_name,
                EXTRACT(DAY FROM date) as day,
                EXTRACT(DOW FROM date) as day_of_week,
                TO_CHAR(date, 'Day') as day_name,
                EXTRACT(WEEK FROM date) as week_of_year,
                CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
            FROM date_range
            ON CONFLICT (date) DO NOTHING
        """)
        
        count = hook.get_first("SELECT COUNT(*) FROM dds_dim_date")[0]
        logger.info(f"✅ Измерение Дата: {count} записей")
        return count
    
    @task
    def transform_user_dimension():
        """Заполнение измерения Пользователь (SCD Type 2) - ИСПРАВЛЕНО"""
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        # Получаем максимальную дату загрузки
        max_loaded = hook.get_first("""
            SELECT COALESCE(MAX(loaded_at), '1900-01-01'::timestamp) 
            FROM dds_dim_users WHERE is_current = TRUE
        """)[0]
        
        # Закрываем старые версии пользователей
        hook.run("""
            UPDATE dds_dim_users 
            SET valid_to = CURRENT_DATE - INTERVAL '1 day', is_current = FALSE
            WHERE is_current = TRUE 
            AND user_id IN (
                SELECT DISTINCT user_id 
                FROM stg_ecommerce_users 
                WHERE loaded_at > %s
            )
        """, parameters=(max_loaded,))
        
        # Вставляем новые версии
        hook.run("""
            INSERT INTO dds_dim_users 
                (user_id, first_name, last_name, email, phone, country, age, age_group, lifetime_value, valid_from, is_current)
            SELECT 
                s.user_id,
                s.first_name,
                s.last_name,
                s.email,
                s.phone,
                s.country,
                s.age,
                CASE 
                    WHEN s.age < 25 THEN '18-24'
                    WHEN s.age < 35 THEN '25-34'
                    WHEN s.age < 50 THEN '35-49'
                    ELSE '50+'
                END as age_group,
                s.lifetime_value,
                CURRENT_DATE as valid_from,
                TRUE as is_current
            FROM stg_ecommerce_users s
            LEFT JOIN dds_dim_users d ON d.user_id = s.user_id AND d.is_current = TRUE
            WHERE d.user_id IS NULL
        """)
        
        count = hook.get_first("SELECT COUNT(*) FROM dds_dim_users")[0]
        logger.info(f"✅ Измерение Пользователь: {count} записей")
        return count
    
    @task
    def transform_product_dimension():
        """Заполнение измерения Товар"""
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        hook.run("""
            INSERT INTO dds_dim_products (product_id, name, category, price, price_category)
            SELECT 
                product_id,
                name,
                category,
                price,
                CASE 
                    WHEN price < 50 THEN 'budget'
                    WHEN price < 200 THEN 'medium'
                    ELSE 'premium'
                END as price_category
            FROM stg_ecommerce_products
            ON CONFLICT (product_id) DO UPDATE SET
                name = EXCLUDED.name,
                category = EXCLUDED.category,
                price = EXCLUDED.price,
                price_category = EXCLUDED.price_category
        """)
        
        count = hook.get_first("SELECT COUNT(*) FROM dds_dim_products")[0]
        logger.info(f"✅ Измерение Товар: {count} записей")
        return count
    
    @task
    def transform_fact_sales():
        """Заполнение факта Продажи"""
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        hook.run("""
            INSERT INTO dds_fact_sales (date_sk, user_sk, product_sk, order_id, quantity, amount)
            SELECT 
                d.date_sk,
                u.user_sk,
                p.product_sk,
                oi.order_id,
                oi.quantity,
                oi.total as amount
            FROM stg_ecommerce_order_items oi
            JOIN stg_ecommerce_orders o ON oi.order_id = o.order_id
            JOIN dds_dim_date d ON d.date = DATE(o.created_at)
            JOIN dds_dim_users u ON u.user_id = o.user_id AND u.is_current = TRUE
            JOIN dds_dim_products p ON p.product_id = oi.product_id
            LEFT JOIN dds_fact_sales fs ON fs.order_id = oi.order_id AND fs.product_sk = p.product_sk
            WHERE fs.order_id IS NULL
        """)
        
        count = hook.get_first("SELECT COUNT(*) FROM dds_fact_sales")[0]
        logger.info(f"✅ Факт Продажи: {count} записей")
        return count
    
    @task
    def transform_fact_events():
        """Заполнение факта События"""
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        hook.run("""
            INSERT INTO dds_fact_events (date_sk, user_sk, event_id, event_type, session_id, page, device, browser, referrer)
            SELECT 
                d.date_sk,
                u.user_sk,
                e.event_id,
                e.event_type,
                e.session_id,
                e.page,
                e.device,
                e.browser,
                e.referrer
            FROM stg_ecommerce_events e
            JOIN dds_dim_date d ON d.date = DATE(e.timestamp)
            JOIN dds_dim_users u ON u.user_id = e.user_id AND u.is_current = TRUE
            LEFT JOIN dds_fact_events fe ON fe.event_id = e.event_id
            WHERE fe.event_id IS NULL
        """)
        
        count = hook.get_first("SELECT COUNT(*) FROM dds_fact_events")[0]
        logger.info(f"✅ Факт События: {count} записей")
        return count
    
    @task
    def log_summary(users_count, products_count, orders_count, events_count, 
                    dim_date, dim_users, dim_products, fact_sales, fact_events):
        logger.info("=" * 60)
        logger.info("📊 PRODUCER DAG COMPLETED")
        logger.info(f"   Staging - Users: {users_count}, Products: {products_count}, Orders: {orders_count}, Events: {events_count}")
        logger.info(f"   DDS - DimDate: {dim_date}, DimUsers: {dim_users}, DimProducts: {dim_products}")
        logger.info(f"   DDS - FactSales: {fact_sales}, FactEvents: {fact_events}")
        logger.info("=" * 60)
    
    @task
    def end_process():
        logger.info("🏁 Завершение E-commerce Producer DAG")
    
    # ============================================================
    # ГРАФ ЗАВИСИМОСТЕЙ
    # ============================================================
    
    start = start_process()
    create_tables = create_postgres_tables()
    
    users = extract_users()
    products = extract_products()
    orders = extract_orders()
    events = extract_events()
    
    users_validated = validate_users(users)
    products_validated = validate_products(products)
    orders_validated = validate_orders(orders)
    events_validated = validate_events(events)
    
    users_loaded = load_users_to_staging(users_validated)
    products_loaded = load_products_to_staging(products_validated)
    orders_loaded = load_orders_to_staging(orders_validated)
    events_loaded = load_events_to_staging(events_validated)
    
    dim_date = transform_date_dimension()
    dim_users = transform_user_dimension()
    dim_products = transform_product_dimension()
    fact_sales = transform_fact_sales()
    fact_events = transform_fact_events()
    
    dataset_published = publish_dataset()
    
    summary = log_summary(users_loaded, products_loaded, orders_loaded, events_loaded,
                          dim_date, dim_users, dim_products, fact_sales, fact_events)
    end = end_process()
    
    start >> create_tables
    create_tables >> [users, products, orders, events]
    
    users >> users_validated >> users_loaded
    products >> products_validated >> products_loaded
    orders >> orders_validated >> orders_loaded
    events >> events_validated >> events_loaded
    
    [users_loaded, products_loaded, orders_loaded, events_loaded] >> dim_date
    [users_loaded, products_loaded, orders_loaded, events_loaded] >> dim_users
    [users_loaded, products_loaded, orders_loaded, events_loaded] >> dim_products
    [dim_date, dim_users, dim_products, orders_loaded] >> fact_sales
    [dim_date, dim_users, events_loaded] >> fact_events
    
    [fact_sales, fact_events] >> dataset_published >> summary >> end


dag = ecommerce_producer_dag()