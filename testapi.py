from api import AbstractDbAPI
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import *

api = AbstractDbAPI
engine = api.create_conn('databaseinf.txt','postgres','localhost','5432')
# df = api.read_sql(engine,'SELECT * FROM users')
# print(df)
# api.create_table(engine,df,'users2')
# api.insert_sql(engine,'users2',df,options='new_table')
# api.truncate_table(engine,'users2')
# api.execute(engine,'DROP TABLE IF EXISTS users2;')

# api.delete_from_table_cond(engine,'users2',"имя='Иван'")
# engine = create_engine("postgresql+psycopg2://postgres:Scpsosat2023@localhost:5432/postgres")
df = api.read_sql(engine,'SELECT * FROM users2')
print(df)
api.drop_table(engine,'users2')
# api.delete_from_table_col(engine,'users2','имя')

