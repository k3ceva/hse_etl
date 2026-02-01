from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(dag_id='hw3_iot_temp') as dag:

    PostgresOperator(
        task_id='iot_temp_cleaned',
        sql="""
            drop table if exists hw3.iot_temp_cleaned;
            create table hw3.iot_temp_cleaned as 
            select
                to_date(noted_date, 'dd-mm-yyyy') as noted_date_dt,
                noted_date,
                "temp"
            from
                hw3.iot_temp_src
            where
                1=1
                and "out/in" = 'In'
                and "temp" between (
                        select percentile_disc(0.05) within group (order by "temp") from hw3.iot_temp_src
                    ) and (
                        select percentile_disc(0.95)  within group (order by "temp") from hw3.iot_temp_src
                    )
            ;
          """,
        postgres_conn_id='de-postgres',
        autocommit=True
    ) >> PostgresOperator(
        task_id='iot_temp_yearly_extr_days',
        sql="""
            drop table if exists hw3.iot_temp_yearly_extr_days;
            create table hw3.iot_temp_yearly_extr_days as
            with min_max_temp_per_day as (
                select
                    noted_date_dt,
                    min("temp") as min_temp,
                    max("temp") as max_temp
                from
                    hw3.iot_temp_cleaned
                group by
                    noted_date_dt
            ),
            min_max_temp_days_flagged as (
                select
                    noted_date_dt,
                    5 >= row_number() over (partition by extract('year' from noted_date_dt) order by min_temp asc, noted_date_dt asc) as min_temp_day,
                    5 >= row_number() over (partition by extract('year' from noted_date_dt) order by max_temp desc, noted_date_dt asc) as max_temp_day
                from
                    min_max_temp_per_day
            )
            select
                noted_date_dt, 'min_temp_day' as day_type
            from
                min_max_temp_days_flagged
            where
                min_temp_day
            union all
            select
                noted_date_dt, 'max_temp_day' as day_type
            from
                min_max_temp_days_flagged
            where
                max_temp_day
            ;
        """,
        postgres_conn_id='de-postgres',
        autocommit=True
    )
