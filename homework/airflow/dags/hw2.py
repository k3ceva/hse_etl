from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

def kek():
    from urllib.parse import quote_plus as quote
    import pymongo

    url = 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
        user=quote('user1'),
        pw=quote('LfAxQ8JsBhT8mtL'),
        hosts=','.join([
            'rc1b-3827efaa1ofdqv70.mdb.yandexcloud.net:27018'
        ]),
        rs='rs01',
        auth_src='db1')

    print(url)
    dbs = pymongo.MongoClient(
        url,
        tls=True,
        tlsAllowInvalidCertificates=True
    )['db1']

    print(dbs.gh.find_one({}))



with DAG(dag_id='kek_dag') as dag:

    PostgresOperator(
        task_id='hi',
        sql="""
            select 1, 2, 4
            ;
          """,
        postgres_conn_id='de-postgres',
        autocommit=True
    ) >> PythonOperator(
        task_id='mongo',
        python_callable = kek,
    )
