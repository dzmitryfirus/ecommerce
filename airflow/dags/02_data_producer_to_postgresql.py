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

# Dataset для триггера Consumer DAG
ECOMMERCE_DATASET = Dataset("postgres://warehouse/stg_ecommerce_data")


# ============================================================
# Pydantic модели для валидации
# ============================================================

class UserModel(BaseModel):
    """Модель пользователя"""
    user_id: str = Field(..., description="UUID пользователя")
    first_name: str = Field(..., min_length=1, max_length=100, description="Имя")
    last_name: str = Field(..., min_length=1, max_length=100, description="Фамилия")
    email: str = Field(..., description="Email")
    phone: str = Field(..., description="Телефон")
    country: str = Field(..., min_length=2, description="Страна")
    age: int = Field(..., ge=18, le=120, description="Возраст")
    registration_date: datetime = Field(..., description="Дата регистрации")
    is_active: bool = Field(..., description="Активен")
    lifetime_value: float = Field(..., ge=0, description="LTV")
    created_at: datetime = Field(..., description="Дата создания записи")
    
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
    """Модель товара"""
    product_id: str = Field(..., pattern=r'^PROD_[A-Z0-9]{8}$', description="ID товара")
    name: str = Field(..., min_length=1, max_length=255, description="Название")
    category: str = Field(..., description="Категория")
    price: float = Field(..., gt=0, le=100000, description="Цена")
    stock: int = Field(..., ge=0, description="Остаток")
    rating: float = Field(..., ge=0, le=5, description="Рейтинг")
    created_at: datetime = Field(..., description="Дата создания")


class OrderItemModel(BaseModel):
    """Модель позиции заказа"""
    product_id: str
    quantity: int = Field(..., ge=1, le=100)
    price: float = Field(..., gt=0)
    total: float = Field(..., gt=0)


class OrderModel(BaseModel):
    """Модель заказа"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    order_id: str = Field(..., pattern=r'^ORD_[A-Z0-9]{12}$')
    user_id: str
    items: List[OrderItemModel]
    total_amount: float = Field(..., gt=0)
    status: str = Field(..., description="pending/processing/shipped/delivered/cancelled/refunded")
    payment_method: str = Field(..., description="card/paypal/apple_pay/google_pay/crypto")
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
    """Модель события"""
    event_id: str = Field(..., description="UUID события")
    user_id: str
    event_type: str = Field(..., description="view_product/add_to_cart/remove_from_cart/start_checkout/purchase")
    session_id: str
    timestamp: datetime
    page: str
    device: str = Field(..., description="mobile/desktop/tablet")
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
    dag_id="02_data_producer_to_postgresql",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "producer", "mongodb", "postgres", "validation"],
    description="Читает данные из MongoDB, валидирует через Pydantic, загружает в PostgreSQL"
)
def ecommerce_producer_dag():
    
    @task(outlets=[ECOMMERCE_DATASET])
    def publish_dataset():
        """Публикация Dataset события"""
        logger.info(f"📡 Публикуем Dataset: {ECOMMERCE_DATASET.uri}")
        return True
    
    @task
    def start_process() -> str:
        """Начало процесса"""
        logger.info("=" * 60)
        logger.info("🚀 Начало работы E-commerce Producer DAG")
        logger.info(f"Время запуска: {datetime.now()}")
        logger.info("=" * 60)
        return "started"
    
    @task
    def create_postgres_tables() -> bool:
        """Создание таблиц в PostgreSQL если не существуют"""
        
        create_tables_sql = """
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
        """
        
        try:
            hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
            hook.run(create_tables_sql)
            logger.info("✅ Таблицы stg_ecommerce_* созданы/проверены в PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при создании таблиц: {e}")
            raise
    
    @task
    def extract_users() -> List[Dict]:
        """Извлечение пользователей из MongoDB"""
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        five_min_ago = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        try:
            client = MongoClient(mongo_uri)
            db = client["source_db"]
            collection = db["ecommerce_users"]
            
            query = {"created_at": {"$gte": five_min_ago}}
            users = list(collection.find(query, {"_id": 0}))
            
            logger.info(f"✅ Извлечено пользователей из MongoDB: {len(users)}")
            return users
        except Exception as e:
            logger.error(f"❌ Ошибка при извлечении пользователей: {e}")
            raise
        finally:
            if 'client' in locals():
                client.close()
    
    @task
    def extract_products() -> List[Dict]:
        """Извлечение товаров из MongoDB"""
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        five_min_ago = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        try:
            client = MongoClient(mongo_uri)
            db = client["source_db"]
            collection = db["ecommerce_products"]
            
            query = {"created_at": {"$gte": five_min_ago}}
            products = list(collection.find(query, {"_id": 0}))
            
            logger.info(f"✅ Извлечено товаров из MongoDB: {len(products)}")
            return products
        except Exception as e:
            logger.error(f"❌ Ошибка при извлечении товаров: {e}")
            raise
        finally:
            if 'client' in locals():
                client.close()
    
    @task
    def extract_orders() -> List[Dict]:
        """Извлечение заказов из MongoDB"""
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        five_min_ago = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        try:
            client = MongoClient(mongo_uri)
            db = client["source_db"]
            collection = db["ecommerce_orders"]
            
            query = {"created_at": {"$gte": five_min_ago}}
            orders = list(collection.find(query, {"_id": 0}))
            
            logger.info(f"✅ Извлечено заказов из MongoDB: {len(orders)}")
            return orders
        except Exception as e:
            logger.error(f"❌ Ошибка при извлечении заказов: {e}")
            raise
        finally:
            if 'client' in locals():
                client.close()
    
    @task
    def extract_events() -> List[Dict]:
        """Извлечение событий из MongoDB"""
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        five_min_ago = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        try:
            client = MongoClient(mongo_uri)
            db = client["source_db"]
            collection = db["ecommerce_events"]
            
            query = {"timestamp": {"$gte": five_min_ago}}
            events = list(collection.find(query, {"_id": 0}))
            
            logger.info(f"✅ Извлечено событий из MongoDB: {len(events)}")
            return events
        except Exception as e:
            logger.error(f"❌ Ошибка при извлечении событий: {e}")
            raise
        finally:
            if 'client' in locals():
                client.close()
    
    @task
    def validate_users(users: List[Dict]) -> Dict[str, Any]:
        """Валидация пользователей через Pydantic"""
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
                logger.warning(f"❌ Ошибка валидации пользователя {user.get('user_id')}: {e}")
        
        logger.info(f"📊 Пользователи: валидных {len(valid_users)}, невалидных {invalid_count}")
        return {
            "total": len(users),
            "valid": len(valid_users),
            "invalid": invalid_count,
            "invalid_by_reason": invalid_by_reason,
            "valid_data": valid_users
        }
    
    @task
    def validate_products(products: List[Dict]) -> Dict[str, Any]:
        """Валидация товаров через Pydantic"""
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
                logger.warning(f"❌ Ошибка валидации товара {product.get('product_id')}: {e}")
        
        logger.info(f"📊 Товары: валидных {len(valid_products)}, невалидных {invalid_count}")
        return {
            "total": len(products),
            "valid": len(valid_products),
            "invalid": invalid_count,
            "invalid_by_reason": invalid_by_reason,
            "valid_data": valid_products
        }
    
    @task
    def validate_orders(orders: List[Dict]) -> Dict[str, Any]:
        """Валидация заказов через Pydantic"""
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
                logger.warning(f"❌ Ошибка валидации заказа {order.get('order_id')}: {e}")
        
        logger.info(f"📊 Заказы: валидных {len(valid_orders)}, невалидных {invalid_count}")
        return {
            "total": len(orders),
            "valid": len(valid_orders),
            "invalid": invalid_count,
            "invalid_by_reason": invalid_by_reason,
            "valid_data": valid_orders
        }
    
    @task
    def validate_events(events: List[Dict]) -> Dict[str, Any]:
        """Валидация событий через Pydantic"""
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
                logger.warning(f"❌ Ошибка валидации события {event.get('event_id')}: {e}")
        
        logger.info(f"📊 События: валидных {len(valid_events)}, невалидных {invalid_count}")
        return {
            "total": len(events),
            "valid": len(valid_events),
            "invalid": invalid_count,
            "invalid_by_reason": invalid_by_reason,
            "valid_data": valid_events
        }
    
    @task
    def upsert_users_to_postgres(validation_result: Dict[str, Any]) -> int:
        """Upsert пользователей в PostgreSQL"""
        valid_users = validation_result.get("valid_data", [])
        
        if not valid_users:
            logger.warning("⚠️ Нет валидных пользователей для загрузки")
            return 0
        
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        upsert_sql = """
        INSERT INTO stg_ecommerce_users 
            (user_id, first_name, last_name, email, phone, country, age, 
             registration_date, is_active, lifetime_value, created_at)
        VALUES (%(user_id)s, %(first_name)s, %(last_name)s, %(email)s, 
                %(phone)s, %(country)s, %(age)s, %(registration_date)s, 
                %(is_active)s, %(lifetime_value)s, %(created_at)s)
        ON CONFLICT (user_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            country = EXCLUDED.country,
            age = EXCLUDED.age,
            lifetime_value = EXCLUDED.lifetime_value,
            is_active = EXCLUDED.is_active,
            loaded_at = NOW()
        """
        
        loaded = 0
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for user in valid_users:
                    cur.execute(upsert_sql, user)
                    loaded += 1
                conn.commit()
        
        logger.info(f"💾 Загружено пользователей в PostgreSQL: {loaded}")
        return loaded
    
    @task
    def upsert_products_to_postgres(validation_result: Dict[str, Any]) -> int:
        """Upsert товаров в PostgreSQL"""
        valid_products = validation_result.get("valid_data", [])
        
        if not valid_products:
            logger.warning("⚠️ Нет валидных товаров для загрузки")
            return 0
        
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        upsert_sql = """
        INSERT INTO stg_ecommerce_products 
            (product_id, name, category, price, stock, rating, created_at)
        VALUES (%(product_id)s, %(name)s, %(category)s, %(price)s, 
                %(stock)s, %(rating)s, %(created_at)s)
        ON CONFLICT (product_id) DO UPDATE SET
            name = EXCLUDED.name,
            category = EXCLUDED.category,
            price = EXCLUDED.price,
            stock = EXCLUDED.stock,
            rating = EXCLUDED.rating,
            loaded_at = NOW()
        """
        
        loaded = 0
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for product in valid_products:
                    cur.execute(upsert_sql, product)
                    loaded += 1
                conn.commit()
        
        logger.info(f"💾 Загружено товаров в PostgreSQL: {loaded}")
        return loaded
    
    @task
    def upsert_orders_to_postgres(validation_result: Dict[str, Any]) -> int:
        """Upsert заказов и их позиций в PostgreSQL"""
        valid_orders = validation_result.get("valid_data", [])
        
        if not valid_orders:
            logger.warning("⚠️ Нет валидных заказов для загрузки")
            return 0
        
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        upsert_orders_sql = """
        INSERT INTO stg_ecommerce_orders 
            (order_id, user_id, total_amount, status, payment_method, created_at)
        VALUES (%(order_id)s, %(user_id)s, %(total_amount)s, %(status)s, %(payment_method)s, %(created_at)s)
        ON CONFLICT (order_id) DO UPDATE SET
            status = EXCLUDED.status,
            payment_method = EXCLUDED.payment_method,
            total_amount = EXCLUDED.total_amount,
            loaded_at = NOW()
        """
        
        upsert_items_sql = """
        INSERT INTO stg_ecommerce_order_items 
            (order_id, product_id, quantity, price, total)
        VALUES (%(order_id)s, %(product_id)s, %(quantity)s, %(price)s, %(total)s)
        ON CONFLICT (order_id, product_id) DO UPDATE SET
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            total = EXCLUDED.total,
            loaded_at = NOW()
        """
        
        loaded_orders = 0
        loaded_items = 0
        
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for order in valid_orders:
                    cur.execute(upsert_orders_sql, order)
                    loaded_orders += 1
                    
                    for item in order.get("items", []):
                        item_data = {
                            "order_id": order["order_id"],
                            "product_id": item["product_id"],
                            "quantity": item["quantity"],
                            "price": item["price"],
                            "total": item["total"]
                        }
                        cur.execute(upsert_items_sql, item_data)
                        loaded_items += 1
                    
                conn.commit()
        
        logger.info(f"💾 Загружено заказов: {loaded_orders}, позиций: {loaded_items}")
        return loaded_orders
    
    @task
    def upsert_events_to_postgres(validation_result: Dict[str, Any]) -> int:
        """Upsert событий в PostgreSQL"""
        valid_events = validation_result.get("valid_data", [])
        
        if not valid_events:
            logger.warning("⚠️ Нет валидных событий для загрузки")
            return 0
        
        hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        
        upsert_sql = """
        INSERT INTO stg_ecommerce_events 
            (event_id, user_id, event_type, session_id, timestamp, page, device, browser, referrer)
        VALUES (%(event_id)s, %(user_id)s, %(event_type)s, %(session_id)s, 
                %(timestamp)s, %(page)s, %(device)s, %(browser)s, %(referrer)s)
        ON CONFLICT (event_id) DO NOTHING
        """
        
        loaded = 0
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for event in valid_events:
                    cur.execute(upsert_sql, event)
                    loaded += 1
                conn.commit()
        
        logger.info(f"💾 Загружено событий в PostgreSQL: {loaded}")
        return loaded
    
    @task
    def log_summary(
        users_result: Dict, products_result: Dict, 
        orders_result: Dict, events_result: Dict
    ):
        """Логирование итоговой статистики"""
        logger.info("=" * 60)
        logger.info("📊 ИТОГОВАЯ СТАТИСТИКА PRODUCER DAG:")
        logger.info(f"   Пользователи: {users_result.get('valid', 0)}/{users_result.get('total', 0)} валидных")
        logger.info(f"   Товары: {products_result.get('valid', 0)}/{products_result.get('total', 0)} валидных")
        logger.info(f"   Заказы: {orders_result.get('valid', 0)}/{orders_result.get('total', 0)} валидных")
        logger.info(f"   События: {events_result.get('valid', 0)}/{events_result.get('total', 0)} валидных")
        logger.info("=" * 60)
    
    @task
    def end_process():
        """Завершение процесса"""
        logger.info("🏁 Завершение E-commerce Producer DAG")
    
    # ============================================================
    # Граф зависимостей
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
    
    users_loaded = upsert_users_to_postgres(users_validated)
    products_loaded = upsert_products_to_postgres(products_validated)
    orders_loaded = upsert_orders_to_postgres(orders_validated)
    events_loaded = upsert_events_to_postgres(events_validated)
    
    dataset_published = publish_dataset()
    
    summary = log_summary(users_validated, products_validated, orders_validated, events_validated)
    end = end_process()
    
    start >> create_tables
    create_tables >> [users, products, orders, events]
    
    users >> users_validated >> users_loaded
    products >> products_validated >> products_loaded
    orders >> orders_validated >> orders_loaded
    events >> events_validated >> events_loaded
    
    [users_loaded, products_loaded, orders_loaded, events_loaded] >> dataset_published >> summary >> end


dag = ecommerce_producer_dag()