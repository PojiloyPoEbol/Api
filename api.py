import datetime
import pandas as pd
from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import *
import psycopg2 as ps

engine = create_engine("postgresql+psycopg2://postgres:Scpsosat2023@localhost:5432/public")
conn = ps.connect(
    host='localhost',
    user='postgres',
    password='Scpsosat2023',
    database='postgres'
)
Base = declarative_base()
Session = sessionmaker(bind=engine)


# Абстрактный класс API
class AbstractDbAPI:
    @staticmethod
    def create_from_df_table(engine, df,table):
        try:
            df.to_sql(f'{table}', con=engine, index=False)
            print(f"Таблица {table} успешно создана")
        except SQLAlchemyError as e:
            print(f"Ошибка при создании таблицы: {e}")

    @staticmethod
    def delete_from_table_cond(engine,table,condition):
        try:
            with Session(bind=engine) as session:
                session.execute(text(f'DELETE FROM {table} WHERE {condition};'))
                session.commit()
                print(f'Из таблицы {table} удалены значения по условию:{condition}')
        except SQLAlchemyError as e:
            print(f'Ошибка при выполнении запроса {e}')


    @staticmethod
    def delete_from_table_col(engine, table, column):
        try:
            with Session(bind=engine) as session:
                session.execute(text(f'ALTER TABLE {table} DROP COLUMN {column};'))
                session.commit()
                print(f'Столбец {column} успешно удален из таблицы {table}')
        except SQLAlchemyError as e:
            print(f"Ошибка при удалении данных: {e}")


    @staticmethod
    def truncate_table(engine,table):
        try:
            with Session(bind=engine) as session:
                session.execute(text(f'TRUNCATE TABLE {table}'))
                session.commit()
            print(f"Таблица {table} успешно очищена")
        except SQLAlchemyError as e:
            print(f"Ошибка при очистке таблицы: {e}")


    @staticmethod
    def read_sql(engine,query):
        try:
            df = pd.read_sql(query,engine)
            return df
        except SQLAlchemyError as e:
            print(f"Ошибка при чтении данных: {e}")


    @staticmethod
    def insert_sql(engine, table, df, options):
        try:
            if options == 'new_table':
                df.to_sql(table, con=engine, index=False)
            elif options == 'append':
                df.to_sql(table, con=engine, index=False, if_exists='append')
            elif options == 'replace':
                df.to_sql(table, con=engine, index=False, if_exists='replace')
            print("Данные успешно записаны")
        except SQLAlchemyError as e:
            print(f"Ошибка при записи данных: {e}")


    @staticmethod
    def execute(engine, query):
        try:
            with Session(bind=engine) as session:
                session.execute(text(query))
                session.commit()
            print("Запрос успешно выполнен")
        except SQLAlchemyError as e:
            print(f"Ошибка при выполнении запроса: {e}")
