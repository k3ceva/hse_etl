from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from pymongo import MongoClient

default_args = {
    'owner': 'k3ceva',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'github_etl',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2015, 1, 1),
    catchup=True,
    max_active_runs=5
)

def create_tables(**context):

    pg_conn = PostgresHook(postgres_conn_id='psql').get_conn()
    pg_cursor = pg_conn.cursor()

    pg_cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS dwh
    """)

    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS dwh.push_events (
            created_at TIMESTAMP NOT NULL,
            actor TEXT NOT NULL,
            repo TEXT NOT NULL,
            ref TEXT,
            commit_count INTEGER,
            PRIMARY KEY (created_at, actor, repo)
        )
    """)
    
    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS dwh.pull_request_events (
            created_at TIMESTAMP NOT NULL,
            actor TEXT NOT NULL,
            repo TEXT NOT NULL,
            pr_number INTEGER,
            action TEXT,
            PRIMARY KEY (created_at, actor, repo)
        )
    """)

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

def extract_transform_load_push(**context):

    conn = BaseHook.get_connection('mongo')
    client = MongoClient(conn.host)
    collection = client[conn.login]['github_history']

    pg_conn = PostgresHook(postgres_conn_id='psql').get_conn()
    pg_cursor = pg_conn.cursor()

    data_interval_start = context['data_interval_start'].strftime('%Y-%m-%dT%H:%M:%SZ')
    data_interval_end = context['data_interval_end'].strftime('%Y-%m-%dT%H:%M:%SZ')
    
    pipeline = [
        {
            '$match': {
                'created_at': {
                    '$gte': data_interval_start,
                    '$lt': data_interval_end
                },
                'type': 'PushEvent'
            }
        },
        {
            '$project': {
                'created_at': 1,
                'actor.login': 1,
                'repo.name': 1,
                'payload.ref': 1,
                'commit_count': {
                    '$size': {
                        '$ifNull': ['$payload.commits', []]
                    }
                }
            }
        },
        {
            '$sort': {
                'created_at': 1
            }
        }
    ]
    
    push_events = collection.aggregate(pipeline)
    
    for event in push_events:
        created_at = event['created_at']
        actor_login = event['actor']['login']
        repo_name = event['repo']['name']
        ref = event['payload'].get('ref', '').replace('refs/heads/', '')
        commit_count = event['commit_count']
        
        pg_cursor.execute(
            """INSERT INTO dwh.push_events 
            (created_at, actor, repo, ref, commit_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (created_at, actor, repo)
            DO UPDATE SET
                ref = EXCLUDED.ref,
                commit_count = EXCLUDED.commit_count""",
            (created_at, actor_login, repo_name, ref, commit_count)
        )
    
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    client.close()

def extract_transform_load_pr(**context):

    conn = BaseHook.get_connection('mongo')
    client = MongoClient(conn.host)
    collection = client[conn.login]['github_history']

    pg_conn = PostgresHook(postgres_conn_id='psql').get_conn()
    pg_cursor = pg_conn.cursor()

    data_interval_start = context['data_interval_start'].strftime('%Y-%m-%dT%H:%M:%SZ')
    data_interval_end = context['data_interval_end'].strftime('%Y-%m-%dT%H:%M:%SZ')
    
    pr_projection = {
        'created_at': 1,
        'actor.login': 1,
        'repo.name': 1,
        'payload.action': 1,
        'payload.number': 1
    }
    
    pr_query = {
        'created_at': {
            '$gte': data_interval_start,
            '$lt': data_interval_end
        },
        'type': 'PullRequestEvent'
    }
    
    pr_events = collection.find(pr_query, pr_projection).sort('created_at', 1)
    
    for event in pr_events:
        created_at = event['created_at']
        actor_login = event['actor']['login']
        repo_name = event['repo']['name']
        action = event['payload']['action']
        pr_number = event['payload']['number']
        
        pg_cursor.execute(
            """INSERT INTO dwh.pull_request_events 
            (created_at, actor, repo, pr_number, action)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (created_at, actor, repo)
            DO UPDATE SET
                pr_number = EXCLUDED.pr_number,
                action = EXCLUDED.action""",
            (created_at, actor_login, repo_name, pr_number, action)
        )

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    client.close()


create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

push_etl_task = PythonOperator(
    task_id='etl_process_push',
    python_callable=extract_transform_load_push,
    dag=dag,
    retries=5,
    retry_delay=timedelta(minutes=1)
)

pr_etl_task = PythonOperator(
    task_id='etl_process_pr',
    python_callable=extract_transform_load_pr,
    dag=dag,
    retries=5,
    retry_delay=timedelta(minutes=1),
)

create_tables_task >> [push_etl_task, pr_etl_task]
