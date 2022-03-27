import psycopg2
from config import config

# Connect ot DB and fetch result set by executing query on DB
def execute_sql(query, sql_limit):
    try:
        conn = psycopg2.connect(**config.config)
    except psycopg2.OperationalError as e:
        print(f'Unable to connect!\n{e}')
        conn = None
    if conn is not None:
        cur = conn.cursor()
        try:
            cur.execute(query)
            cols = [desc[0] for desc in cur.description]
            if sql_limit is None:
                rows = cur.fetchall()
            else:
                rows = cur.fetchmany(sql_limit)
            return cols, rows
        except Exception as e:
            print(e)
            return "SQLError", "failed"
        finally:
            cur.close()
            conn.close()
    else:
        return "ConnError", "failed"


def fetch_resulset(query, sql_limit):
    ret_col, ret_row = execute_sql(query, sql_limit)
    if ret_col == 'SQLError':
        return failed_flag, "Could not fetch Results. Failed to execute SQL. Check SQL statement"
    if ret_col == 'ConnError':
        return failed_flag, "Failed to connect to Database. Check connection details."
    return  ret_col, ret_row

