# E-commerce Analytics Pipeline

ETL-пайплайн для аналитики интернет-магазина. Система автоматически генерирует данные о пользователях, товарах, заказах и событиях, обрабатывает их через оркестратор Airflow, загружает в PostgreSQL и ClickHouse, и предоставляет визуализацию ключевых бизнес-метрик в BI-дашбордах.

# Архитектура
Data Generator (Python)
         │
         ▼
      MongoDB (Raw Data)
         │
         ▼
    Apache Airflow (Orchestration)
         │
         ├──► PostgreSQL (DWH: raw → DDS)
         │
         └──► ClickHouse (Analytical Marts)
                   │
                   ▼
        PowerBI / Metabase / Tableau (Dashboards)



docker compose up
