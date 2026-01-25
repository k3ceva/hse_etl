from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(dag_id='hw2_parse_json_pets') as dag:

    PostgresOperator(
        task_id='v_pets_src_unnested',
        sql="""
            create or replace view hw2.v_pets_src_unnested as
            select
                jsonb_array_elements(src -> 'pets') as json_rows
            from
                hw2.json_data
            ;
          """,
        postgres_conn_id='de-postgres',
        autocommit=True
    ) >> [
        PostgresOperator(
            task_id='pets',
            sql="""
                drop table if exists hw2.pets;
                create table hw2.pets as
                select 
                    json_rows ->> 'name' as name,
                    json_rows ->> 'species' as species,
                    cast(json_rows -> 'birthYear' as int) as birthYear,
                    json_rows ->> 'photo' as photo_href
                from
                    hw2.v_pets_src_unnested
                ;
            """,
            postgres_conn_id='de-postgres',
            autocommit=True
        ),
        PostgresOperator(
            task_id='pets_fav_foods',
            sql="""
                drop table if exists hw2.pets_fav_foods;
                create table hw2.pets_fav_foods as
                select 
                    json_rows ->> 'name' as name,
                    jsonb_array_elements_text(json_rows -> 'favFoods') as favFoods
                from
                    hw2.v_pets_src_unnested
                ;
            """,
            postgres_conn_id='de-postgres',
            autocommit=True
        ),
    ]
