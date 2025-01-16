import pandas as pd
import sqlalchemy
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import (Column, DateTime, Float, Integer, MetaData, String,
                        Table)


def create_table():
    metadata_obj = MetaData()
    hook = PostgresHook('destination_db')
    db_conn = hook.get_sqlalchemy_engine()
    users_churn = Table(
        'alt_users_churn',
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("customer_id", String(200)),
        Column("begin_date", DateTime()),
        Column("end_date", DateTime()),
        Column("type", String(200)),
        Column("paperless_billing", String(200)),
        Column("payment_method", String(200)),
        Column("monthly_charges", Float),
        Column("total_charges", Float()),
        Column("internet_service", String(200)),
        Column("online_security", String(200)),
        Column("online_backup", String(200)),
        Column("device_protection", String(200)),
        Column("tech_support", String(200)),
        Column("streaming_tv", String(200)),
        Column("streaming_movies", String(200)),
        Column("gender", String(200)),
        Column("senior_citizen", Integer()),
        Column("partner", String(200)),
        Column("dependents", String(200)),
        Column("multiple_lines", String(200)),
        Column("target", Integer()),
        sqlalchemy.schema.UniqueConstraint("customer_id")
    )
    metadata_obj.create_all(db_conn)


def extract(**kwargs):

    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    sql = """
        select
            c.customer_id, c.begin_date, c.end_date,
            c.type, c.paperless_billing, c.payment_method,
            c.monthly_charges, c.total_charges, i.internet_service,
            i.online_security, i.online_backup, i.device_protection,
            i.tech_support, i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.partner, p.dependents,
            ph.multiple_lines
        from contracts as c
        left join internet as i on i.customer_id = c.customer_id
        left join personal as p on p.customer_id = c.customer_id
        left join phone as ph on ph.customer_id = c.customer_id
    """
    data = pd.read_sql(sql, conn)
    conn.close()
    ti = kwargs['ti']
    ti.xcom_push('extracted_data', data)


def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')
    data['target'] = (data['end_date'] != 'No').astype(int)
    data['end_date'].replace({'No': None}, inplace=True)
    ti.xcom_push('transformed_data', data)


def load(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="alt_users_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['customer_id'],
        rows=data.values.tolist()
    )
