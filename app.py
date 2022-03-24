import os, json, flask
from  flask_restful import reqparse
from flask import Flask, request, render_template
from flask_restful import Resource, Api
import pandas as pd
from fetch_data import fetch_data, fetch_data_custom
from dataviz.data_viz import generate_charts
from config import config

app = Flask(__name__)
api = Api(app)

sql_parser = reqparse.RequestParser()
sql_parser.add_argument('limit', type=int, help="Limit the returned rows of SQL Query")

# custom_sql_args = reqparse.RequestParser()
# custom_sql_args.add_argument("sql", type=str, help="Send SQL Query", required=True)

@app.route('/')
@app.route("/home")
def home_page():
    return render_template('home.html')


@app.route("/list_api")
def list_api_page():
    return render_template('list_api.html', api_list_items=[fname.split('.')[0] for fname in os.listdir(config.project_sql) if os.path.isdir(fname) == False])


@app.route("/fetch_apps/<string:tablename>", methods=['GET'])
def fetchdata_apps(tablename):
    #tablename='accnt'
    sql_args = sql_parser.parse_args()
    if sql_args['limit'] == '' or sql_args['limit'] is None:
        df = pd.DataFrame(fetch_data(tablename, None))
    else:
        df = pd.DataFrame(fetch_data(tablename, sql_args['limit']))
    return flask.jsonify(json.loads(df.to_json(orient='records')))


class FetchApp(Resource):
    def __init__(self):
        pass

    def get(self):
        #directory = config.project_sql
        api_list = [fname.split('.')[0] for fname in os.listdir(config.project_sql) if os.path.isdir(fname) == False]
        return flask.jsonify(api_list)


class GetViz(Resource):
    def __init__(self):
        pass

    def get(self):

        #directory = config.project_sql
        sql_file_list = [fname for fname in os.listdir(config.project_sql) if os.path.isdir(fname) == False and fname.endswith('.sql')]
        viz_file = generate_charts(sql_file_list)
        #print(viz_file)
        if '.' in viz_file:
            if viz_file.split('.')[1] == 'pdf' or viz_file.split('.')[1] == 'zip':
                return flask.send_file(viz_file)
            else:
                return viz_file
        else:
            return viz_file



class CustomSql(Resource):
    def __init__(self):
        pass

    # def get(self, custsql):
    #    return custsql

    def post(self):
        data = request.get_json()
        sql_args = sql_parser.parse_args()
        if sql_args['limit'] == '' or sql_args['limit'] is None:
            custom_data = fetch_data_custom(data, None)
        else:
            custom_data = fetch_data_custom(data, sql_args['limit'])
        return json.loads(custom_data)


api.add_resource(FetchApp, '/fetch_apps')
api.add_resource(CustomSql, '/customsql')
api.add_resource(GetViz, '/getviz')
if __name__ == '__main__':
    port = 5000
    app.run(debug=True, host='0.0.0.0', port=port)
