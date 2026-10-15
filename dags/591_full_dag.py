import datetime
import time
import psycopg2
import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
from psycopg2.extras import execute_values


# ============================================================
# CONNECTIONS
# ============================================================

api_conn = BaseHook.get_connection('create_files_api')
api_endpoint = api_conn.host
api_token = api_conn.password

nickname = 'alexasia87'
cohort = '19'

headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}


psql_conn = BaseHook.get_connection('pg_connection')

conn = psycopg2.connect(
    f"dbname='de' "
    f"port='{psql_conn.port}' "
    f"user='{psql_conn.login}' "
    f"host='{psql_conn.host}' "
    f"password='{psql_conn.password}'"
)

conn.close()


# ============================================================
# CREATE REPORT
# ============================================================

def create_files_request(ti, api_endpoint, headers):

    method_url = '/generate_report'

    r = requests.post(
        'https://' + api_endpoint + method_url,
        headers=headers
    )

    response_dict = json.loads(r.content)

    ti.xcom_push(
        key='task_id',
        value=response_dict['task_id']
    )

    print('task_id:', response_dict['task_id'])

    return response_dict['task_id']


# ============================================================
# CHECK REPORT
# ============================================================

def check_report(ti, api_endpoint, headers):

    task_ids = ti.xcom_pull(
        key='task_id',
        task_ids=['create_files_request']
    )

    task_id = task_ids[0]

    method_url = '/get_report'

    payload = {
        'task_id': task_id
    }

    report_id = None

    for i in range(4):

        time.sleep(70)

        r = requests.get(
            'https://' + api_endpoint + method_url,
            params=payload,
            headers=headers
        )

        response_dict = json.loads(r.content)

        print(i, response_dict['status'])

        if response_dict['status'] == 'SUCCESS':
            report_id = response_dict['data']['report_id']
            break

    if report_id is None:
        raise Exception('Report was not generated')

    ti.xcom_push(
        key='report_id',
        value=report_id
    )

    print('report_id:', report_id)

    return report_id


# ============================================================
# UPLOAD FROM S3 TO POSTGRES
# ============================================================

def upload_from_s3_to_pg(ti, nickname, cohort):

    report_ids = ti.xcom_pull(
        key='report_id',
        task_ids=['check_ready_report']
    )

    report_id = report_ids[0]

    storage_url = (
        'https://storage.yandexcloud.net/'
        's3-sprint3/cohort_{COHORT_NUMBER}/'
        '{NICKNAME}/project/{REPORT_ID}/{FILE_NAME}'
    )

    personal_storage_url = (
        storage_url
        .replace('{COHORT_NUMBER}', str(cohort))
        .replace('{NICKNAME}', nickname)
        .replace('{REPORT_ID}', str(report_id))
    )

    # ========================================================
    # POSTGRESQL CONNECTION
    # ========================================================

    psql_conn = BaseHook.get_connection('pg_connection')

    conn = psycopg2.connect(
        f"dbname='student' "
        f"port='{psql_conn.port}' "
        f"user='{psql_conn.login}' "
        f"host='{psql_conn.host}' "
        f"password='{psql_conn.password}'"
    )

    cur = conn.cursor()

    try:
        # Добавляем поля для исходных идентификаторов.
        # Старые данные при этом не удаляются.
        cur.execute("""
            ALTER TABLE stage.user_order_log
                ADD COLUMN IF NOT EXISTS source_id BIGINT;

            ALTER TABLE stage.user_order_log
                ADD COLUMN IF NOT EXISTS uniq_id TEXT;
        """)

        # ====================================================
        # CUSTOMER RESEARCH
        # ====================================================

        df_customer_research = pd.read_csv(
            personal_storage_url.replace(
                '{FILE_NAME}',
                'customer_research.csv'
            )
        )

        cr_columns = [
            'date_id',
            'category_id',
            'geo_id',
            'sales_qty',
            'sales_amt'
        ]

        df_customer_research = (
            df_customer_research[cr_columns]
            .astype(object)
            .where(
                pd.notna(df_customer_research[cr_columns]),
                None
            )
        )

        cr_rows = [
            tuple(
                value.item()
                if isinstance(value, np.generic)
                else value
                for value in row
            )
            for row in df_customer_research.itertuples(
                index=False,
                name=None
            )
        ]

        if cr_rows:
            execute_values(
                cur,
                """
                INSERT INTO stage.customer_research
                (
                    date_id,
                    category_id,
                    geo_id,
                    sales_qty,
                    sales_amt
                )
                VALUES %s
                """,
                cr_rows,
                page_size=1000
            )

        # ====================================================
        # USER ORDER LOG
        # ====================================================

        df_order_log = pd.read_csv(
            personal_storage_url.replace(
                '{FILE_NAME}',
                'user_order_log.csv'
            )
        )

        # Поддержка старого формата без status.
        if 'status' not in df_order_log.columns:
            df_order_log['status'] = 'shipped'
        else:
            df_order_log['status'] = (
                df_order_log['status']
                .fillna('shipped')
                .astype(str)
                .str.lower()
            )

        # Сохраняем исходный ID отдельно от
        # внутреннего ID PostgreSQL.
        if 'id' in df_order_log.columns:
            df_order_log = df_order_log.rename(
                columns={'id': 'source_id'}
            )
        else:
            df_order_log['source_id'] = None

        if 'uniq_id' not in df_order_log.columns:
            df_order_log['uniq_id'] = None

        uol_columns = [
            'source_id',
            'uniq_id',
            'date_time',
            'city_id',
            'city_name',
            'customer_id',
            'first_name',
            'last_name',
            'item_id',
            'item_name',
            'quantity',
            'payment_amount',
            'status'
        ]

        df_order_log = (
            df_order_log[uol_columns]
            .astype(object)
            .where(
                pd.notna(df_order_log[uol_columns]),
                None
            )
        )

        uol_rows = [
            tuple(
                value.item()
                if isinstance(value, np.generic)
                else value
                for value in row
            )
            for row in df_order_log.itertuples(
                index=False,
                name=None
            )
        ]

        if uol_rows:
            execute_values(
                cur,
                """
                INSERT INTO stage.user_order_log
                (
                    source_id,
                    uniq_id,
                    date_time,
                    city_id,
                    city_name,
                    customer_id,
                    first_name,
                    last_name,
                    item_id,
                    item_name,
                    quantity,
                    payment_amount,
                    status
                )
                VALUES %s
                """,
                uol_rows,
                page_size=1000
            )

        # ====================================================
        # USER ACTIVITY LOG
        # ====================================================

        df_activity_log = pd.read_csv(
            personal_storage_url.replace(
                '{FILE_NAME}',
                'user_activity_log.csv'
            )
        )

        ual_columns = [
            'date_time',
            'action_id',
            'customer_id',
            'quantity'
        ]

        df_activity_log = (
            df_activity_log[ual_columns]
            .astype(object)
            .where(
                pd.notna(df_activity_log[ual_columns]),
                None
            )
        )

        ual_rows = [
            tuple(
                value.item()
                if isinstance(value, np.generic)
                else value
                for value in row
            )
            for row in df_activity_log.itertuples(
                index=False,
                name=None
            )
        ]

        if ual_rows:
            execute_values(
                cur,
                """
                INSERT INTO stage.user_activity_log
                (
                    date_time,
                    action_id,
                    customer_id,
                    quantity
                )
                VALUES %s
                """,
                ual_rows,
                page_size=1000
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()

    return 200


# ============================================================
# UPDATE DIMENSION TABLES
# ============================================================

def update_mart_d_tables(ti):

    psql_conn = BaseHook.get_connection(
        'pg_connection'
    )

    conn = psycopg2.connect(
        f"dbname='student' "
        f"port='{psql_conn.port}' "
        f"user='{psql_conn.login}' "
        f"host='{psql_conn.host}' "
        f"password='{psql_conn.password}'"
    )

    cur = conn.cursor()

    try:
        # ====================================================
        # D_CALENDAR
        # ====================================================

        cur.execute("""
            WITH all_dates AS (
                SELECT date_time::date AS fact_date
                FROM stage.user_order_log

                UNION

                SELECT date_time::date AS fact_date
                FROM stage.user_activity_log

                UNION

                SELECT date_id::date AS fact_date
                FROM stage.customer_research
            )

            INSERT INTO mart.d_calendar
            (
                date_id,
                fact_date,
                day_num,
                month_num,
                month_name,
                year_num
            )
            SELECT
                TO_CHAR(fact_date, 'YYYYMMDD')::integer,
                fact_date,
                EXTRACT(DAY FROM fact_date)::integer,
                EXTRACT(MONTH FROM fact_date)::integer,
                TO_CHAR(fact_date, 'Mon'),
                EXTRACT(YEAR FROM fact_date)::integer
            FROM all_dates
            WHERE fact_date IS NOT NULL

            ON CONFLICT (date_id) DO UPDATE SET
                fact_date = EXCLUDED.fact_date,
                day_num = EXCLUDED.day_num,
                month_num = EXCLUDED.month_num,
                month_name = EXCLUDED.month_name,
                year_num = EXCLUDED.year_num;
        """)

        # ====================================================
        # D_CUSTOMER
        # ====================================================

        cur.execute("""
            INSERT INTO mart.d_customer
            (
                customer_id,
                first_name,
                last_name,
                city_id
            )
            SELECT
                customer_id::integer,
                MAX(first_name),
                MAX(last_name),
                MAX(city_id)
            FROM stage.user_order_log
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id

            ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                city_id = EXCLUDED.city_id;
        """)

        # ====================================================
        # D_ITEM
        # ====================================================

        cur.execute("""
            INSERT INTO mart.d_item
            (
                item_id,
                item_name
            )
            SELECT
                item_id,
                MAX(item_name)
            FROM stage.user_order_log
            WHERE item_id IS NOT NULL
            GROUP BY item_id

            ON CONFLICT (item_id) DO UPDATE SET
                item_name = EXCLUDED.item_name;
        """)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()

    return 200


# ============================================================
# UPDATE FACT TABLES
# ============================================================

def update_mart_f_tables(ti):

    psql_conn = BaseHook.get_connection('pg_connection')

    conn = psycopg2.connect(
        f"dbname='student' "
        f"port='{psql_conn.port}' "
        f"user='{psql_conn.login}' "
        f"host='{psql_conn.host}' "
        f"password='{psql_conn.password}'"
    )

    cur = conn.cursor()

    try:
        # F_ACTIVITY
        cur.execute("""
            INSERT INTO mart.f_activity
            (
                activity_id,
                date_id,
                click_number
            )
            SELECT
                ual.action_id,
                dc.date_id,
                SUM(ual.quantity)
            FROM stage.user_activity_log AS ual
            JOIN mart.d_calendar AS dc
                ON dc.fact_date = ual.date_time::date
            GROUP BY
                ual.action_id,
                dc.date_id;
        """)

        # F_SALES
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart.f_sales
            (
                transaction_id BIGSERIAL PRIMARY KEY,
                date_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                price NUMERIC(14, 2),
                quantity BIGINT NOT NULL,
                payment_amount NUMERIC(14, 2) NOT NULL,
                status VARCHAR(20) NOT NULL,

                FOREIGN KEY (date_id)
                    REFERENCES mart.d_calendar(date_id),

                FOREIGN KEY (item_id)
                    REFERENCES mart.d_item(item_id),

                FOREIGN KEY (customer_id)
                    REFERENCES mart.d_customer(customer_id)
            );
        """)

        cur.execute("TRUNCATE TABLE mart.f_sales RESTART IDENTITY;")

        cur.execute("""
            INSERT INTO mart.f_sales
            (
                date_id,
                item_id,
                customer_id,
                price,
                quantity,
                payment_amount,
                status
            )
            SELECT
                dc.date_id,
                uol.item_id,
                uol.customer_id::INTEGER,
                ROUND(
                    ABS(uol.payment_amount)
                    / NULLIF(ABS(uol.quantity), 0),
                    2
                ),
                CASE
                    WHEN LOWER(COALESCE(uol.status, 'shipped')) = 'refunded'
                        THEN -ABS(uol.quantity)
                    ELSE ABS(uol.quantity)
                END,
                CASE
                    WHEN LOWER(COALESCE(uol.status, 'shipped')) = 'refunded'
                        THEN -ABS(uol.payment_amount)
                    ELSE ABS(uol.payment_amount)
                END,
                LOWER(COALESCE(uol.status, 'shipped'))
            FROM stage.user_order_log AS uol
            JOIN mart.d_calendar AS dc
                ON dc.fact_date = uol.date_time::date
            WHERE uol.item_id IS NOT NULL
              AND uol.customer_id IS NOT NULL;
        """)

        # F_CUSTOMER_RETENTION
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart.f_customer_retention
            (
                new_customers_count INTEGER NOT NULL,
                returning_customers_count INTEGER NOT NULL,
                refunded_customer_count INTEGER NOT NULL,
                period_name VARCHAR(20) NOT NULL,
                period_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                new_customers_revenue NUMERIC(15, 2) NOT NULL,
                returning_customers_revenue NUMERIC(15, 2) NOT NULL,
                customers_refunded INTEGER NOT NULL,
                PRIMARY KEY (period_id, item_id)
            );
        """)

        cur.execute("DELETE FROM mart.f_customer_retention;")

        cur.execute("""
            WITH customer_week AS (
                SELECT
                    DATE_TRUNC('week', dc.fact_date)::date AS week_start,
                    fs.customer_id,
                    COUNT(*) AS orders_count
                FROM mart.f_sales AS fs
                JOIN mart.d_calendar AS dc
                    ON dc.date_id = fs.date_id
                WHERE fs.status <> 'refunded'
                GROUP BY
                    DATE_TRUNC('week', dc.fact_date)::date,
                    fs.customer_id
            ),
            sales_with_customer_type AS (
                SELECT
                    DATE_TRUNC('week', dc.fact_date)::date AS week_start,
                    fs.item_id,
                    fs.customer_id,
                    fs.status,
                    fs.payment_amount,
                    COALESCE(cw.orders_count, 0) AS orders_count
                FROM mart.f_sales AS fs
                JOIN mart.d_calendar AS dc
                    ON dc.date_id = fs.date_id
                LEFT JOIN customer_week AS cw
                    ON cw.week_start =
                       DATE_TRUNC('week', dc.fact_date)::date
                   AND cw.customer_id = fs.customer_id
            )
            INSERT INTO mart.f_customer_retention
            (
                new_customers_count,
                returning_customers_count,
                refunded_customer_count,
                period_name,
                period_id,
                item_id,
                new_customers_revenue,
                returning_customers_revenue,
                customers_refunded
            )
            SELECT
                COUNT(DISTINCT customer_id)
                    FILTER (WHERE orders_count = 1),
                COUNT(DISTINCT customer_id)
                    FILTER (WHERE orders_count > 1),
                COUNT(DISTINCT customer_id)
                    FILTER (WHERE status = 'refunded'),
                'weekly',
                TO_CHAR(week_start, 'IYYYIW')::INTEGER,
                item_id,
                COALESCE(
                    SUM(payment_amount)
                        FILTER (WHERE orders_count = 1),
                    0
                ),
                COALESCE(
                    SUM(payment_amount)
                        FILTER (WHERE orders_count > 1),
                    0
                ),
                COUNT(*)
                    FILTER (WHERE status = 'refunded')
            FROM sales_with_customer_type
            GROUP BY week_start, item_id;
        """)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()

    return 200


# ============================================================
# DAG
# ============================================================

dag = DAG(
    dag_id='591_full_dag',
    schedule_interval=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(
        minutes=60
    )
)


# ============================================================
# TASK 1
# ============================================================

t_file_request = PythonOperator(
    task_id='create_files_request',

    python_callable=create_files_request,

    op_kwargs={
        'api_endpoint': api_endpoint,
        'headers': headers
    },

    dag=dag
)


# ============================================================
# TASK 2
# ============================================================

t_check_report = PythonOperator(
    task_id='check_ready_report',

    python_callable=check_report,

    op_kwargs={
        'api_endpoint': api_endpoint,
        'headers': headers
    },

    dag=dag
)


# ============================================================
# TASK 3
# ============================================================

t_upload_from_s3_to_pg = PythonOperator(
    task_id='upload_from_s3_to_pg',

    python_callable=upload_from_s3_to_pg,

    op_kwargs={
        'nickname': nickname,
        'cohort': cohort
    },

    dag=dag
)


# ============================================================
# TASK 4
# ============================================================

t_update_mart_d_tables = PythonOperator(
    task_id='update_mart_d_tables',

    python_callable=update_mart_d_tables,

    dag=dag
)


# ============================================================
# TASK 5
# ============================================================

t_update_mart_f_tables = PythonOperator(
    task_id='update_mart_f_tables',

    python_callable=update_mart_f_tables,

    dag=dag
)


# ============================================================
# DEPENDENCIES
# ============================================================

t_file_request >> \
t_check_report >> \
t_upload_from_s3_to_pg >> \
t_update_mart_d_tables >> \
t_update_mart_f_tables