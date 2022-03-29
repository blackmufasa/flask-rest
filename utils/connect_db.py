import psycopg2
from config import config
import pandas as pd


def create_pandas_table(sql_query, conn):
    table = pd.read_sql_query(sql_query, conn)
    return table

# Connect ot DB and fetch result set by executing query on DB
def execute_sql(query, sql_args):
    try:
        conn = psycopg2.connect(**config.config)
    except psycopg2.OperationalError as e:
        print(f'Unable to connect!\n{e}')
        conn = None
    if conn is not None:
        cur = conn.cursor()
        try:
            #cur.execute('Select * from ( ' + query + ' ) as AaZz ' + sql_args)
            #cur.execute(query)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows,columns=cols)
            return "success", df
            # table_result = create_pandas_table('Select * from ( ' + query + ' ) as AaZz ' + sql_args, conn)
            # return "success", table_result

        except Exception as e:
            print(e)
            return "failed", "SQLError"
        finally:
            cur.close()
            conn.close()
    else:
        return "failed", "ConnError"


def fetch_resulset(query, sql_args):
    ret_flag, ret_result = execute_sql(query, sql_args)
    if ret_flag == 'failed':
        if str(ret_result) == 'SQLError':
            return "failed", "Could not fetch Results. Failed to execute SQL. Check SQL statement"
        if str(ret_result) == 'ConnError':
            return "failed", "Failed to connect to Database. Check connection details."
    else:
        return "success", ret_result

