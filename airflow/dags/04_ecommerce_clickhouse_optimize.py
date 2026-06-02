from datetime import datetime
import logging
from airflow.decorators import dag, task
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

CLICKHOUSE_CONFIG = {
    'host': 'clickhouse',
    'port': 9000,
    'user': 'airflow',
    'password': 'airflow',
    'database': 'airflow'
}


@dag(
    dag_id="04_ecommerce_clickhouse_optimize",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",  # Каждую ночь в 2 часа
    catchup=False,
    tags=["ecommerce", "clickhouse", "optimize"],
    description="Оптимизация ClickHouse таблиц с ReplacingMergeTree"
)
def ecommerce_clickhouse_optimize():
    
    @task
    def optimize_tables():
        """Принудительное слияние таблиц для удаления дубликатов"""
        ch = Client(**CLICKHOUSE_CONFIG)
        
        logger.info("🚀 Запуск OPTIMIZE для таблиц ClickHouse")
        
        ch.execute("OPTIMIZE TABLE dm_category_performance FINAL")
        logger.info("✅ OPTIMIZE TABLE dm_category_performance FINAL")
        
        ch.execute("OPTIMIZE TABLE dm_user_behavior FINAL")
        logger.info("✅ OPTIMIZE TABLE dm_user_behavior FINAL")
        
        # Проверка размера таблиц после оптимизации
        size_result = ch.execute("""
            SELECT 
                table,
                formatReadableSize(sum(bytes)) as size
            FROM system.parts
            WHERE table IN ('dm_category_performance', 'dm_user_behavior')
            AND active
            GROUP BY table
        """)
        
        for row in size_result:
            logger.info(f"   Таблица {row[0]}: {row[1]}")
        
        ch.disconnect()
        return True
    
    @task
    def log_optimize_result():
        """Логирование результата оптимизации"""
        logger.info("=" * 60)
        logger.info("✅ ClickHouse OPTIMIZE completed")
        logger.info("=" * 60)
    
    optimize = optimize_tables()
    log_result = log_optimize_result()
    
    optimize >> log_result


dag = ecommerce_clickhouse_optimize()