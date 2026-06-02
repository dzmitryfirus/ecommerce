# E-commerce Analytics Pipeline

ETL-пайплайн для аналитики интернет-магазина. Данные генерируются, обрабатываются через Airflow, загружаются в PostgreSQL (Staging + DDS), строятся витрины в ClickHouse и визуализируются в Power BI.

# Архитектура
 Airflow (Generator → Producer → Consumer) → MongoDB (Source) → PostgreSQL (DDS) → ClickHouse (Marts) → Power BI

# DAG:
01_generator — генерация данных в MongoDB (каждую минуту)
02_producer — MongoDB → PostgreSQL Staging → DDS (каждые 5 минут)
03_consumer — PostgreSQL DDS → ClickHouse витрины (по триггеру)
04_optimize — оптимизация ClickHouse (ежедневно в 2:00)

# Модель данных (DDS — звездная схема)
Измерения:
- dds_dim_date — календарь (дата, год, месяц, день недели)
- dds_dim_users — пользователи (SCD Type 2, возрастные группы)
- dds_dim_products — товары (категория, ценовой сегмент)
Факты:
- dds_fact_sales — продажи (связь дата → пользователь → товар)
- dds_fact_events — события (просмотры, добавления в корзину)

# ClickHouse витрины
Витрина	                  Содержание
dm_daily_sales	            выручка, заказы, покупатели, средний чек по дням
dm_category_performance	   продажи по категориям
dm_user_behavior	         активность, заказы, LTV, даты первой/последней активности
dm_top_products	         топ-20 товаров по выручке
dm_daily_active_users	   DAU, WAU, удержание, новые пользователи


# Power BI меры (DAX)
Total Revenue = SUM(dm_daily_sales[total_revenue])
Total Orders = SUM(dm_daily_sales[total_orders])
Avg Order Value = AVERAGE(dm_daily_sales[avg_order_value])

DAU = SUM(dm_daily_active_users[dau])
Retention = AVERAGE(dm_daily_active_users[dau_wau_ratio]) * 100
Avg LTV = AVERAGE(dm_user_behavior[total_spent])


# Запустить
docker-compose up 
Скопировать DAG-файлы в airflow/dags/


# Автор
Dmitry Firus

