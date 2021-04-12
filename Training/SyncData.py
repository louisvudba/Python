import psycopg2 as psy
import pandas as pd
import pyodbc
import csv
from io import StringIO
from sqlalchemy import create_engine
import time
import psycopg2

def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connect()
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


start = time.time()
conn = pyodbc.connect('Driver={SQL Server};Server=LAMVT1FINTECH\SQL2019;Database=StackOverflow2013;Trusted_Connection=yes;')
df = pd.read_sql_query('select top 100 id,aboutme,age,creationdate,displayname,downvotes,emailhash,lastaccessdate,location,reputation,upvotes,views,websiteurl,accountid from Users', conn)
end = time.time()
print(end - start)

engine = create_engine(
    'postgresql+psycopg2://postgres:Hh010898@@@localhost:5432/stackoverflow')

with engine.connect() as conn:
    df1 = pd.read_sql_query(
        'select id,aboutme,age,creationdate,displayname,downvotes,emailhash,lastaccessdate,location,reputation,upvotes,views,websiteurl,accountid from users limit 100', conn)

print(df1)
#df.to_sql('Users', engine, method=psql_insert_copy)
#df.to_sql('users', con=engine, if_exists='replace', method=psql_insert_copy)


pg_conn = psy.connect(
    "host=localhost dbname=stackoverflow user=postgres password=Hh010898@@")
cur = pg_conn.cursor()
cur.execute('SELECT * FROM public.users')
