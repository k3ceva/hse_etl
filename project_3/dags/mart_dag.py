from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'k3ceva',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'calc_github_marts',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2015, 1, 1),
    catchup=True,
    max_active_runs=1
)

def create_marts(**context):
    
    pg_conn = PostgresHook(postgres_conn_id='psql').get_conn()
    pg_cursor = pg_conn.cursor()
    
    pg_cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS dm;
        CREATE SCHEMA IF NOT EXISTS dm_stg;
    """)
    
    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS dm.user_activity (
            event_date DATE NOT NULL,
            actor TEXT NOT NULL,
            push_count INTEGER DEFAULT 0,
            pr_count INTEGER DEFAULT 0,
            total_commits INTEGER DEFAULT 0,
            last_activity TIMESTAMP
        ) PARTITION BY LIST (event_date);
    """)

    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS dm.repository_stats (
            event_date DATE NOT NULL,
            repo TEXT NOT NULL,
            total_pushes INTEGER DEFAULT 0,
            total_commits INTEGER DEFAULT 0,
            last_push TIMESTAMP,
            unique_contributors INTEGER DEFAULT 0,
            total_pr_opened INTEGER DEFAULT 0,
            last_pr_opened_number INTEGER,
            last_pr_opened_at TIMESTAMP,
            total_pr_closed INTEGER DEFAULT 0,
            last_pr_closed_number INTEGER,
            last_pr_closed_at TIMESTAMP
        ) PARTITION BY LIST (event_date);
    """)

    data_interval_start = context['data_interval_start'].strftime('%Y-%m-%d')

    pg_cursor.execute(f"""
        DROP TABLE IF EXISTS dm_stg.user_activity;
        CREATE TABLE dm_stg.user_activity (
            LIKE dm.user_activity INCLUDING ALL
        ) PARTITION BY LIST (event_date);
        DROP TABLE IF EXISTS dm_stg.user_activity_{data_interval_start.replace('-', '_')};
        CREATE TABLE dm_stg.user_activity_{data_interval_start.replace('-', '_')} PARTITION OF dm_stg.user_activity FOR VALUES IN ('{data_interval_start}');

        DROP TABLE IF EXISTS dm_stg.repository_stats;
        CREATE TABLE dm_stg.repository_stats (
            LIKE dm.repository_stats INCLUDING ALL
        ) PARTITION BY LIST (event_date);
        DROP TABLE IF EXISTS dm_stg.repository_stats_{data_interval_start.replace('-', '_')};
        CREATE TABLE dm_stg.repository_stats_{data_interval_start.replace('-', '_')} PARTITION OF dm_stg.repository_stats FOR VALUES IN ('{data_interval_start}')
    """)
    
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

def calc_marts(**context):

    pg_conn = PostgresHook(postgres_conn_id='psql').get_conn()
    pg_cursor = pg_conn.cursor()
    
    data_interval_start = context['data_interval_start'].strftime('%Y-%m-%d')
    data_interval_end = context['data_interval_end'].strftime('%Y-%m-%d')
    
    pg_cursor.execute("""
        INSERT INTO dm_stg.user_activity (event_date, actor, pr_count, push_count, total_commits, last_activity)
        SELECT
            %s::DATE as event_date,
            actor,
            SUM(pr_count) as pr_count,
            SUM(push_count) as push_count,
            SUM(total_commits) as total_commits,
            MAX(last_activity) as last_activity
        FROM (SELECT
                actor,
                COUNT(*) as pr_count,
                NULL as push_count,
                NULL as total_commits,
                MAX(created_at) as last_activity
            FROM dwh.pull_request_events 
            WHERE created_at >= %s AND created_at < %s
            GROUP BY actor
            UNION ALL
            SELECT
                actor,
                NULL as pr_count,
                COUNT(*) as push_count,
                SUM(commit_count) as total_commits,
                MAX(created_at) as last_activity
            FROM dwh.push_events 
            WHERE created_at >= %s AND created_at < %s
            GROUP BY actor) sq
        GROUP BY actor
    """, (data_interval_start, data_interval_start, data_interval_end, data_interval_start, data_interval_end))
    
    pg_cursor.execute(f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dm' AND tablename = 'user_activity_{data_interval_start.replace('-', '_')}') THEN
                EXECUTE 'ALTER TABLE dm.user_activity DETACH PARTITION dm.user_activity_{data_interval_start.replace('-', '_')}';
                EXECUTE 'DROP TABLE dm.user_activity_{data_interval_start.replace('-', '_')}';
            END IF;
        END $$;

        ALTER TABLE dm_stg.user_activity DETACH PARTITION dm_stg.user_activity_{data_interval_start.replace('-', '_')};
        ALTER TABLE dm_stg.user_activity_{data_interval_start.replace('-', '_')} SET SCHEMA dm;
        ALTER TABLE dm.user_activity ATTACH PARTITION dm.user_activity_{data_interval_start.replace('-', '_')} FOR VALUES IN ('{data_interval_start}');
    """)
    
    pg_cursor.execute("""
        INSERT INTO dm_stg.repository_stats (
            event_date, repo, total_pushes, total_commits, last_push, unique_contributors, total_pr_opened, last_pr_opened_number, last_pr_opened_at, total_pr_closed, last_pr_closed_number, last_pr_closed_at
        )
        SELECT
            %s::DATE as event_date,
            repo,
            SUM(total_pushes) as total_pushes,
            SUM(total_commits) as total_commits,
            MAX(last_push) as last_push,
            SUM(unique_contributors) as unique_contributors,
            SUM(total_pr_opened) as total_pr_opened,
            MAX(last_pr_opened_number) as last_pr_opened_number,
            MAX(last_pr_opened_at) as last_pr_opened_at,
            SUM(total_pr_closed) as total_pr_closed,
            MAX(last_pr_closed_number) as last_pr_closed_number,
            MAX(last_pr_closed_at) as last_pr_closed_at
        FROM (SELECT
                repo,
                COUNT(*) as total_pushes,
                SUM(commit_count) as total_commits,
                MAX(created_at) as last_push,
                COUNT(DISTINCT actor) as unique_contributors,
                NULL as total_pr_opened,
                NULL as total_pr_closed,
                NULL as last_pr_opened_at,
                NULL as last_pr_closed_at,
                NULL as last_pr_opened_number,
                NULL as last_pr_closed_number
            FROM dwh.push_events 
            WHERE created_at >= %s AND created_at < %s
            GROUP BY repo
            UNION ALL
            SELECT
                repo,
                NULL as total_pushes,
                NULL as total_commits,
                NULL as last_push,
                NULL as unique_contributors,
                SUM(CASE WHEN action = 'opened' THEN 1 END) as total_pr_opened,
                SUM(CASE WHEN action = 'closed' THEN 1 END) as total_pr_closed,
                MAX(CASE WHEN action = 'opened' THEN created_at END) as last_pr_opened_at,
                MAX(CASE WHEN action = 'closed' THEN created_at END) as last_pr_closed_at,
                MAX(CASE WHEN action = 'opened' THEN pr_number END) as last_pr_opened_number,
                MAX(CASE WHEN action = 'closed' THEN pr_number END) as last_pr_closed_number
            FROM dwh.pull_request_events 
            WHERE created_at >= %s AND created_at < %s
            GROUP BY repo) sq
        GROUP BY repo
    """, (data_interval_start, data_interval_start, data_interval_end, data_interval_start, data_interval_end))

    pg_cursor.execute(f"""
                      
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'dm' AND tablename = 'repository_stats_{data_interval_start.replace('-', '_')}') THEN
                EXECUTE 'ALTER TABLE dm.repository_stats DETACH PARTITION dm.repository_stats_{data_interval_start.replace('-', '_')}';
                EXECUTE 'DROP TABLE dm.repository_stats_{data_interval_start.replace('-', '_')}';
            END IF;
        END $$;

        ALTER TABLE dm_stg.repository_stats DETACH PARTITION dm_stg.repository_stats_{data_interval_start.replace('-', '_')};
        ALTER TABLE dm_stg.repository_stats_{data_interval_start.replace('-', '_')} SET SCHEMA dm;
        ALTER TABLE dm.repository_stats ATTACH PARTITION dm.repository_stats_{data_interval_start.replace('-', '_')} FOR VALUES IN ('{data_interval_start}');
    """)
    
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

def cleanup_temp_tables(**context):

    pg_conn = PostgresHook(postgres_conn_id='psql').get_conn()
    pg_cursor = pg_conn.cursor()
    
    pg_cursor.execute("""
        DROP TABLE IF EXISTS dm_stg.user_activity;
        DROP TABLE IF EXISTS dm_stg.repository_stats;
    """)
    
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()


create_marts_task = PythonOperator(
    task_id='create_marts',
    python_callable=create_marts,
    dag=dag,
)

calc_marts_task = PythonOperator(
    task_id='calc_marts',
    python_callable=calc_marts,
    dag=dag,
    retries=5,
    retry_delay=timedelta(minutes=1)
)

cleanup_temp_tables_task = PythonOperator(
    task_id='cleanup_temp_tables',
    python_callable=cleanup_temp_tables,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

create_marts_task >> calc_marts_task >> cleanup_temp_tables_task
