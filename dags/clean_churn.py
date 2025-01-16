# скопируйте код ниже #

# dags/clean_churn.py

import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"]
)
def clean_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table():
        from sqlalchemy import Table, Column, DateTime, Float, Integer, Index, MetaData, String, UniqueConstraint, inspect
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        churn_table = Table(
            'clean_users_churn',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('customer_id', String),
            Column('begin_date', DateTime),
            Column('end_date', DateTime),
            Column('type', String),
            Column('paperless_billing', String),
            Column('payment_method', String),
            Column('monthly_charges', Float),
            Column('total_charges', Float),
            Column('internet_service', String),
            Column('online_security', String),
            Column('online_backup', String),
            Column('device_protection', String),
            Column('tech_support', String),
            Column('streaming_tv', String),
            Column('streaming_movies', String),
            Column('gender', String),
            Column('senior_citizen', Integer),
            Column('partner', String),
            Column('dependents', String),
            Column('multiple_lines', String),
            Column('target', Integer),
            UniqueConstraint('customer_id', name='unique_clean_customer_constraint')
        )
        if not inspect(db_engine).has_table(churn_table.name):
            metadata.create_all(db_engine)

    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select
            customer_id, begin_date, end_date, type, paperless_billing, payment_method, monthly_charges, total_charges,
            internet_service, online_security, online_backup, device_protection, tech_support, streaming_tv, streaming_movies,
            gender, senior_citizen, partner, dependents,
            multiple_lines
        from users_churn
        """
        data = pd.read_sql(sql, conn, parse_dates=['start_date', 'end_date'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):

        def remove_duplicates(data):
            feature_cols = data.columns.drop('customer_id').tolist()
            is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
            data = data[~is_duplicated_features].reset_index(drop=True)
            return data 

        def fill_missing_values(data):
            cols_with_nans = data.isnull().sum()
            cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')
            for col in cols_with_nans:
                if data[col].dtype in [float, int]:
                    fill_value = data[col].mean()
                elif data[col].dtype == 'object':
                    fill_value = data[col].mode().iloc[0]
                data[col] = data[col].fillna(fill_value)
            return data 

        def drop_emission(data):
            num_cols = data.select_dtypes(['float']).columns
            threshold = 1.5

            for col in num_cols:
                Q1 = data[col].quantile(q=0.25)
                Q3 = data[col].quantile(q=0.75)
                IQR = Q3 -Q1
                margin = threshold*IQR
                lower = Q1 - margin
                upper = Q3 + margin
                data = data.loc[data[col].between(lower, upper)]
            return data
        
        data = remove_duplicates(data)
        data = fill_missing_values(data)
        data = drop_emission(data)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        data['end_date'] = data['end_date'].astype('object').replace(np.nan, None)
        hook.insert_rows(
            table='clean_users_churn',
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_churn_dataset()
