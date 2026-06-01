from datetime import datetime, timezone, timedelta
import logging
import random
import uuid
from pymongo import MongoClient
from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger(__name__)

# Constants
VALID_CATEGORIES = [
    "Electronics", "Clothing", "Books", "Home & Garden", "Sports",
    "Toys", "Beauty", "Automotive", "Food", "Pet Supplies"
]
VALID_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "refunded"]
VALID_PAYMENT_METHODS = ["card", "paypal", "apple_pay", "google_pay", "crypto"]
VALID_EVENT_TYPES = ["view_product", "add_to_cart", "remove_from_cart", "start_checkout", "purchase"]
VALID_DEVICES = ["mobile", "desktop", "tablet"]
VALID_BROWSERS = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]

FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
COUNTRIES = ["USA", "UK", "Germany", "France", "Canada", "Australia", "Japan", "Brazil", "India", "Spain", "Italy", "Mexico"]


@dag(
    dag_id="01_data_generator_to_mongodb",
    start_date=datetime(2026, 1, 1),
    schedule="* * * * *",  # Every 1 min
    catchup=False,
    tags=["ecommerce", "generator", "mongodb"],
    description="Генерирует данные для MongoDB"
)
def data_generator_to_mongodb():
    
    @task
    def generate_users():
        """Users in MongoDB"""
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        
        num_users = random.randint(5, 15)
        users = []
        
        for _ in range(num_users):
            user = {
                "user_id": str(uuid.uuid4()),
                "first_name": random.choice(FIRST_NAMES),
                "last_name": random.choice(LAST_NAMES),
                "email": f"user_{uuid.uuid4().hex[:8]}@example.com",
                "phone": f"+{random.randint(1, 99)}{random.randint(100000000, 999999999)}",
                "country": random.choice(COUNTRIES),
                "age": random.randint(18, 70),
                "registration_date": datetime.now(timezone.utc) - timedelta(days=random.randint(0, 730)),
                "is_active": random.choice([True, False]),
                "lifetime_value": round(random.uniform(0, 50000), 2),
                "created_at": datetime.now(timezone.utc)
            }
            users.append(user)
        
        try:
            with MongoClient(mongo_uri) as client:
                db = client["source_db"]
                collection = db["ecommerce_users"]
                inserted = collection.insert_many(users, ordered=False)
                
                logger.info(f"✅ Сгенерировано пользователей: {len(inserted.inserted_ids)}")
                return len(inserted.inserted_ids)
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            raise
    
    @task
    def generate_products():
        """Products in MongoDB"""
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        
        num_products = random.randint(10, 25)
        products = []
        
        for _ in range(num_products):
            category = random.choice(VALID_CATEGORIES)
            product = {
                "product_id": f"PROD_{uuid.uuid4().hex[:8].upper()}",
                "name": f"{category} Item {random.randint(1, 1000)}",
                "category": category,
                "price": round(random.uniform(10, 2000), 2),
                "stock": random.randint(0, 500),
                "rating": round(random.uniform(3, 5), 1),
                "created_at": datetime.now(timezone.utc)
            }
            products.append(product)
        
        try:
            with MongoClient(mongo_uri) as client:
                db = client["source_db"]
                collection = db["ecommerce_products"]
                inserted = collection.insert_many(products, ordered=False)
                
                logger.info(f"✅ Сгенерировано товаров: {len(inserted.inserted_ids)}")
                return len(inserted.inserted_ids)
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            raise
    
    @task
    def generate_orders():
        """Orders in MongoDB"""
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        
        try:
            with MongoClient(mongo_uri) as client:
                db = client["source_db"]
                users = list(db["ecommerce_users"].find({}, {"user_id": 1, "_id": 0}))
                products = list(db["ecommerce_products"].find({"price": {"$gt": 0}}, {"product_id": 1, "price": 1, "_id": 0}))
                
                if not users or not products:
                    logger.warning("Нет данных для создания заказов")
                    return 0
                
                user_ids = [u["user_id"] for u in users]
                products_list = [(p["product_id"], p["price"]) for p in products]
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return 0
        
        num_orders = random.randint(5, 20)
        orders = []
        
        for _ in range(num_orders):
            user_id = random.choice(user_ids)
            num_items = random.randint(1, 4)
            order_items = []
            total = 0
            
            for _ in range(num_items):
                product_id, price = random.choice(products_list)
                quantity = random.randint(1, 3)
                item_total = price * quantity
                total += item_total
                order_items.append({
                    "product_id": product_id,
                    "quantity": quantity,
                    "price": price,
                    "total": round(item_total, 2)
                })
            
            orders.append({
                "order_id": f"ORD_{uuid.uuid4().hex[:12].upper()}",
                "user_id": user_id,
                "items": order_items,
                "total_amount": round(total, 2),
                "status": random.choice(VALID_STATUSES),
                "payment_method": random.choice(VALID_PAYMENT_METHODS),
                "created_at": datetime.now(timezone.utc) - timedelta(hours=random.randint(0, 72))
            })
        
        try:
            with MongoClient(mongo_uri) as client:
                db = client["source_db"]
                collection = db["ecommerce_orders"]
                inserted = collection.insert_many(orders, ordered=False)
                
                logger.info(f"✅ Сгенерировано заказов: {len(inserted.inserted_ids)}")
                return len(inserted.inserted_ids)
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            raise
    
    @task
    def generate_events():
        """Events in MongoDB"""
        mongo_uri = Variable.get("mongo_uri", default_var="mongodb://airflow:airflow@mongodb:27017/")
        
        try:
            with MongoClient(mongo_uri) as client:
                db = client["source_db"]
                users = list(db["ecommerce_users"].find({}, {"user_id": 1, "_id": 0}))
                if not users:
                    logger.warning("Нет пользователей для генерации событий")
                    return 0
                user_ids = [u["user_id"] for u in users]
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return 0
        
        num_events = random.randint(20, 100)
        events = []
        
        for _ in range(num_events):
            events.append({
                "event_id": str(uuid.uuid4()),
                "user_id": random.choice(user_ids),
                "event_type": random.choice(VALID_EVENT_TYPES),
                "session_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 60)),
                "page": random.choice(["/", "/catalog", "/cart", "/checkout", "/profile"]),
                "device": random.choice(VALID_DEVICES),
                "browser": random.choice(VALID_BROWSERS),
                "referrer": random.choice(["google", "facebook", "direct", "email"])
            })
        
        try:
            with MongoClient(mongo_uri) as client:
                db = client["source_db"]
                collection = db["ecommerce_events"]
                inserted = collection.insert_many(events, ordered=False)
                
                logger.info(f"✅ Сгенерировано событий: {len(inserted.inserted_ids)}")
                return len(inserted.inserted_ids)
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            raise
    
    # Параллельный запуск всех генераторов
    generate_users()
    generate_products()
    generate_orders()
    generate_events()


dag = data_generator_to_mongodb()