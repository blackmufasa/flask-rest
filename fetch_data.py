import os, json
import pandas as pd
import flask
from utils.connect_db import fetch_resulset

#Fetch data based on sql files palced in sql folder
def fetch_data(filename, sql_args):
    #print(filename)
    row_dict = pd.DataFrame.from_dict({"data": ["Not Found"]})
    directory = './sql'
    full_filename = os.path.join(directory, filename+'.sql')
    if os.path.isfile(full_filename) and full_filename.endswith('.sql'):
        print(f"Running data fetch for : {full_filename}")
        with open(f'{full_filename}', 'r') as sq:
            query = sq.read().replace('\n', ' ')
        #query = query+" and super_region='APAC' LIMIT 10"
        cols, rows = fetch_resulset(query, sql_args)
        if cols == 'failed':
            error_d = pd.DataFrame.from_dict({"error": ["Error while fetching data"]})
            return error_d
        #print(rows)
        #df = pd.DataFrame(rows)
        #f = rows
        #row_dict = rows.to_json(orient='records')
        #print(row_dict)
        #return json.loads(row_dict)
        return rows
    else:
        #return json.loads(row_dict)
        return row_dict


#fetch custom sql data
def fetch_data_custom(data, sql_args):
    query = data[list(data)[0]]
    cols, rows = fetch_resulset(query, sql_args)
    if cols == 'failed':
        error_d = pd.DataFrame.from_dict({"error": ["Error while fetching data"]})
        # return rows
        return error_d
    return rows
    #df = rows
    #row_dict = df.to_json(orient='records')
    #return json.loads(row_dict)
