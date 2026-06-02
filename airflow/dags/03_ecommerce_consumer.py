from datetime import datetime
import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

ECOMMERCE_DATASET = Dataset("postgres://warehouse/dds_ecommerce_data")

CLICKHOUSE_CONFIG = {
    'host': 'clickhouse',
    'port': 9000,
    'user': 'airflow',
    'password': 'airflow',
    'database': 'airflow'
}


@dag(
    dag_id="03_ecommerce_consumer_postgres_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=[ECOMMERCE_DATASET],
    catchup=False,
    tags=["ecommerce", "consumer", "clickhouse", "dds"],
    description="Забирает данные из DDS слоя PostgreSQL и загружает в ClickHouse витрины"
)
def ecommerce_consumer_dag():
    
    @task
    def create_clickhouse_tables():
        """Создание таблиц в ClickHouse если не существуют"""
        ch = Client(**CLICKHOUSE_CONFIG)
        
        ch.execute("""
            CREATE TABLE IF NOT EXISTS dm_daily_sales (
                sale_date Date,
                total_revenue Float64,
                total_orders UInt64,
                unique_customers UInt64,
                avg_order_value Float64,
                loaded_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY sale_date
        """)
        
        ch.execute("""
            CREATE TABLE IF NOT EXISTS dm_category_performance (
                category String,
                total_units_sold UInt64,
                total_revenue Float64,
                avg_price Float64,
                total_orders UInt64,
                loaded_at DateTime
            ) ENGINE = ReplacingMergeTree(loaded_at)
            ORDER BY category
        """)
        
        ch.execute("""
            CREATE TABLE IF NOT EXISTS dm_top_products (
                product_id String,
                product_name String,
                category String,
                total_quantity_sold UInt64,
                total_revenue Float64,
                rank UInt64,
                loaded_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY rank
        """)
        
        ch.execute("""
            CREATE TABLE IF NOT EXISTS dm_daily_active_users (
                date Date,
                dau UInt64,
                wau UInt64,
                dau_wau_ratio Float64,
                new_users UInt64,
                loaded_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY date
        """)
        
        logger.info("✅ Все таблицы в ClickHouse созданы/проверены")
        ch.disconnect()
        return True
    
    @task
    def load_daily_sales():
        """Загрузка ежедневных продаж в ClickHouse"""
        pg = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        ch = Client(**CLICKHOUSE_CONFIG)
        
        ch.execute("ALTER TABLE dm_daily_sales DELETE WHERE sale_date >= today() - 30")
        
        result = pg.get_records("""
            SELECT 
                d.date as sale_date,
                SUM(fs.amount) as total_revenue,
                COUNT(DISTINCT fs.order_id) as total_orders,
                COUNT(DISTINCT fs.user_sk) as unique_customers,
                AVG(fs.amount) as avg_order_value
            FROM dds_fact_sales fs
            JOIN dds_dim_date d ON fs.date_sk = d.date_sk
            WHERE d.date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY d.date
            ORDER BY d.date DESC
        """)
        
        if not result:
            logger.warning("Нет данных для daily_sales")
            return 0
        
        for row in result:
            ch.execute(f"""
                INSERT INTO dm_daily_sales 
                (sale_date, total_revenue, total_orders, unique_customers, avg_order_value, loaded_at)
                VALUES ('{row[0]}', {row[1]}, {row[2]}, {row[3]}, {row[4]}, now())
            """)
        
        logger.info(f"✅ Загружено {len(result)} записей в dm_daily_sales")
        ch.disconnect()
        return len(result)
    
    @task
    def load_category_performance():
        """Загрузка эффективности категорий в ClickHouse"""
        pg = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        ch = Client(**CLICKHOUSE_CONFIG)
        
        result = pg.get_records("""
            SELECT 
                p.category,
                SUM(fs.quantity) as total_units_sold,
                SUM(fs.amount) as total_revenue,
                AVG(p.price) as avg_price,
                COUNT(DISTINCT fs.order_id) as total_orders
            FROM dds_fact_sales fs
            JOIN dds_dim_products p ON fs.product_sk = p.product_sk
            JOIN dds_dim_date d ON fs.date_sk = d.date_sk
            WHERE d.date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY p.category
            ORDER BY total_revenue DESC
        """)
        
        if not result:
            logger.warning("Нет данных для category_performance")
            return 0
        
        for row in result:
            ch.execute(f"""
                INSERT INTO dm_category_performance 
                (category, total_units_sold, total_revenue, avg_price, total_orders, loaded_at)
                VALUES ('{row[0]}', {row[1]}, {row[2]}, {row[3]}, {row[4]}, now())
            """)
        
        logger.info(f"✅ Загружено {len(result)} записей в dm_category_performance")
        ch.disconnect()
        return len(result)
    
    @task
    def load_user_behavior():
        """Загрузка поведения пользователей в ClickHouse (исправлено)"""
        pg = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        ch = Client(**CLICKHOUSE_CONFIG)
        
        # Пересоздаём таблицу с правильной структурой
        ch.execute("DROP TABLE IF EXISTS dm_user_behavior")
        
        ch.execute("""
            CREATE TABLE IF NOT EXISTS dm_user_behavior (
                user_id String,
                total_events UInt64,
                total_sessions UInt64,
                total_orders UInt64,
                total_spent Float64,
                first_active_date Date,
                last_active_date Date,
                loaded_at DateTime
            ) ENGINE = ReplacingMergeTree(loaded_at)
            ORDER BY user_id
        """)
        
        result = pg.get_records("""
            WITH user_events AS (
                SELECT 
                    u.user_id,
                    COUNT(DISTINCT fe.event_id) as events_count,
                    COUNT(DISTINCT fe.session_id) as sessions_count,
                    MIN(d.date) as first_active,
                    MAX(d.date) as last_active
                FROM dds_fact_events fe
                JOIN dds_dim_users u ON fe.user_sk = u.user_sk
                JOIN dds_dim_date d ON fe.date_sk = d.date_sk
                WHERE u.is_current = TRUE
                GROUP BY u.user_id
            ),
            user_orders AS (
                SELECT 
                    u.user_id,
                    COUNT(DISTINCT fs.order_id) as orders_count,
                    SUM(fs.amount) as spent_amount
                FROM dds_fact_sales fs
                JOIN dds_dim_users u ON fs.user_sk = u.user_sk
                WHERE u.is_current = TRUE
                GROUP BY u.user_id
            )
            SELECT 
                COALESCE(ue.user_id, uo.user_id) as user_id,
                COALESCE(ue.events_count, 0) as events_count,
                COALESCE(ue.sessions_count, 0) as sessions_count,
                COALESCE(uo.orders_count, 0) as orders_count,
                COALESCE(uo.spent_amount, 0) as spent_amount,
                COALESCE(ue.first_active, CURRENT_DATE) as first_active_date,
                COALESCE(ue.last_active, CURRENT_DATE) as last_active_date
            FROM user_events ue
            FULL OUTER JOIN user_orders uo ON ue.user_id = uo.user_id
        """)
        
        if not result:
            logger.warning("Нет данных для user_behavior")
            return 0
        
        for row in result:
            ch.execute(f"""
                INSERT INTO dm_user_behavior 
                (user_id, total_events, total_sessions, total_orders, total_spent, 
                 first_active_date, last_active_date, loaded_at)
                VALUES (
                    '{row[0]}', {row[1]}, {row[2]}, {row[3]}, {row[4]}, 
                    '{row[5]}', '{row[6]}', now()
                )
            """)
        
        ch.execute("OPTIMIZE TABLE dm_user_behavior FINAL")
        
        count = ch.execute("SELECT COUNT(*) FROM dm_user_behavior")[0][0]
        logger.info(f"✅ Загружено {count} записей в dm_user_behavior")
        
        ch.disconnect()
        return count
    
    @task
    def load_top_products():
        """Загрузка топ товаров в ClickHouse"""
        pg = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        ch = Client(**CLICKHOUSE_CONFIG)
        
        ch.execute("TRUNCATE TABLE dm_top_products")
        
        result = pg.get_records("""
            WITH product_stats AS (
                SELECT 
                    p.product_id,
                    p.name as product_name,
                    p.category,
                    SUM(fs.quantity) as total_quantity_sold,
                    SUM(fs.amount) as total_revenue,
                    ROW_NUMBER() OVER (ORDER BY SUM(fs.amount) DESC) as rank
                FROM dds_fact_sales fs
                JOIN dds_dim_products p ON fs.product_sk = p.product_sk
                JOIN dds_dim_date d ON fs.date_sk = d.date_sk
                WHERE d.date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY p.product_id, p.name, p.category
            )
            SELECT * FROM product_stats
            WHERE rank <= 20
            ORDER BY rank
        """)
        
        if not result:
            logger.warning("Нет данных для top_products")
            return 0
        
        for row in result:
            ch.execute(f"""
                INSERT INTO dm_top_products 
                (product_id, product_name, category, total_quantity_sold, total_revenue, rank, loaded_at)
                VALUES ('{row[0]}', '{row[1]}', '{row[2]}', {row[3]}, {row[4]}, {row[5]}, now())
            """)
        
        logger.info(f"✅ Загружено {len(result)} записей в dm_top_products")
        ch.disconnect()
        return len(result)
    
    @task
    def load_daily_active_users():
        """Загрузка DAU/WAU метрик в ClickHouse"""
        pg = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        ch = Client(**CLICKHOUSE_CONFIG)
        
        ch.execute("ALTER TABLE dm_daily_active_users DELETE WHERE date >= today() - 30")
        
        result = pg.get_records("""
            WITH daily_users AS (
                SELECT 
                    d.date,
                    COUNT(DISTINCT fe.user_sk) as dau
                FROM dds_fact_events fe
                JOIN dds_dim_date d ON fe.date_sk = d.date_sk
                WHERE d.date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY d.date
            ),
            new_users AS (
                SELECT 
                    valid_from as date,
                    COUNT(*) as new_users
                FROM dds_dim_users
                WHERE valid_from >= CURRENT_DATE - INTERVAL '30 days' AND is_current = TRUE
                GROUP BY valid_from
            )
            SELECT 
                d.date,
                d.dau,
                COALESCE(n.new_users, 0) as new_users
            FROM daily_users d
            LEFT JOIN new_users n ON d.date = n.date
            ORDER BY d.date DESC
        """)
        
        if not result:
            logger.warning("Нет данных для daily_active_users")
            return 0
        
        for row in result:
            ch.execute(f"""
                INSERT INTO dm_daily_active_users 
                (date, dau, wau, dau_wau_ratio, new_users, loaded_at)
                VALUES ('{row[0]}', {row[1]}, 0, 0, {row[2]}, now())
            """)
        
        logger.info(f"✅ Загружено {len(result)} записей в dm_daily_active_users")
        ch.disconnect()
        return len(result)
    
    @task
    def log_summary(daily_count, category_count, user_count, products_count, dau_count):
        """Логирование итоговой статистики"""
        logger.info("=" * 60)
        logger.info("📊 CONSUMER DAG COMPLETED")
        logger.info(f"   dm_daily_sales: {daily_count} records")
        logger.info(f"   dm_category_performance: {category_count} records")
        logger.info(f"   dm_user_behavior: {user_count} records")
        logger.info(f"   dm_top_products: {products_count} records")
        logger.info(f"   dm_daily_active_users: {dau_count} records")
        logger.info("=" * 60)
    
    # Граф зависимостей
    create_tables = create_clickhouse_tables()
    
    daily_sales = load_daily_sales()
    category_perf = load_category_performance()
    user_behavior = load_user_behavior()
    top_products = load_top_products()
    daily_users = load_daily_active_users()
    
    summary = log_summary(daily_sales, category_perf, user_behavior, top_products, daily_users)
    
    create_tables >> [daily_sales, category_perf, user_behavior, top_products, daily_users] >> summary


dag = ecommerce_consumer_dag()