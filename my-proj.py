import re
import click
from dagster import MetadataValue, Output, asset, job, op
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import urlparse
import csv
import requests


def get_sql_conn():
    """return db connection."""
    #get password from environmnet var
    conn = psycopg2.connect(user="postgres",
                            # пароль, который указали при установке PostgreSQL
                            password="QWERTY345p",
                            host="127.0.0.1",
                            port="5432",
                            database="Dagster")
    try:
        return conn
    except:
        print("Error loading the config file.")


def get_postgres_creds():
    #get password from environmnet var
    pwd = 'QWERTY345p'
    uid = 'postgres'
    server = '127.0.0.1'
    db = 'Dagster'
    port = 5432
    cs = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
    try:
        return cs
    except:
        print("Error loading the config file.")




# df - датасет вводимый пользователем в формате csv, original_tbl - имя таблицы, в которую загрузиться датасет
@op
def load_to_db(df, original_tbl):
    # df = pd.DataFrame(df)
    #data = {'id': ['1', '2'], 'name': ['hello', 'world'], 'url': ['http://hello.com/home', 'https://world.org/']}
    # df = pd.DataFrame(data)
    original_tbl = 'Test'
    engine = get_postgres_creds()
    conn = get_sql_conn()
    # Курсор для выполнения операций с базой данных
    conn.autocommit = True
    cursor = conn.cursor()
    # Распечатать сведения о PostgreSQL
    print("Информация о сервере PostgreSQL")
    print(conn.get_dsn_parameters(), "\n")
    # Выполнение SQL-запроса
    cursor.execute("SELECT version();")
    # Получить результат
    record = cursor.fetchone()
    print("Вы подключены к - ", record, "\n")



    df.to_sql(original_tbl, engine, if_exists='replace')

    sql1 = '''select * from public."Test";'''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)
    # метаданные для отображения таблицы в пользовательском интерфейсе Dagster
    metadata = {
        "num_records": len(df),
        "preview": MetadataValue.md(df[["id", "name", "url"]].to_markdown()),
    }
    return Output(value=df, metadata=metadata)


# original_tbl - таблица, в которой хранится изначальный датасет, new_table - новая таблица с дополнительной колонкой
# содержащей домен
@op
def sql_create_table_as(original_tbl, new_table):

    engine = get_postgres_creds()
    conn = get_sql_conn()
    cursor = conn.cursor()

    df = pd.read_sql_query(f'select * FROM {original_tbl}', conn)
    df['domain_of_url'] = df['url']
    df['domain_of_url'] = df['domain_of_url'].apply((lambda x: urlparse(x).netloc))

    df.to_sql(new_table, engine, if_exists='replace')
    sql1 = f''''select * from public."{new_table}";'''''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)

    # метаданные для отображения таблицы в пользовательском интерфейсе Dagster
    metadata = {
        "num_records": len(df),
        "preview": MetadataValue.md(df[["id", "name", "url", "domain_of_url"]].to_markdown()),
    }
    return Output(value=df, metadata=metadata)


# table - таблица, из которой пользователь хочет вывести данные в файл, file - название файла
@op
def copy_to_file(table, file):
    engine = get_postgres_creds()
    conn = get_sql_conn()
    cursor = conn.cursor()

    sql1 = f''''select * from public."{table}";'''''
    cursor.execute(sql1)

    with open(file, 'w') as f:
        writer = csv.writer(f)
        for row in cursor.fetchall():
            writer.writerow(row)

# @click.command()
# run - функция для запуска всех тасков
@job
def run():
    load_to_db()
