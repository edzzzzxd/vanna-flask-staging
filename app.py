

from functools import wraps
from flask import Flask, jsonify, Response, request, redirect, url_for
from werkzeug.security import generate_password_hash, check_password_hash
import flask
import os
from dotenv import load_dotenv
load_dotenv()
from cache import MemoryCache
from db import Database

app = Flask(__name__, static_url_path='')


# SETUP
cache = MemoryCache()

# from vanna.local import LocalContext_OpenAI
# vn = LocalContext_OpenAI()

from vanna.remote import VannaDefault
from vanna.flask import VannaFlaskApp

# vn = VannaDefault(model=os.environ['VANNA_MODEL'], api_key=os.environ['VANNA_API_KEY'])
# vn = VannaDefault(model='jormodel', api_key='d8a6af0b998948c1bbf5b2cc92c7e2bf')
vn = VannaDefault(model=os.getenv('VANNA_MODEL'), api_key=os.getenv('VANNA_API_KEY'))
vn.connect_to_mysql(host=os.getenv('DB_HOST'), dbname=os.getenv('DB_DATABASE'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'), port=int(os.getenv('DB_PORT')))
# vn.connect_to_sqlite('https://vanna.ai/Chinook.sqlite')
# vn.connect_to_snowflake(
#     account=os.environ['SNOWFLAKE_ACCOUNT'],
#     username=os.environ['SNOWFLAKE_USERNAME'],
#     password=os.environ['SNOWFLAKE_PASSWORD'],
#     database=os.environ['SNOWFLAKE_DATABASE'],
#     warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
# )

db = Database()

# NO NEED TO CHANGE ANYTHING BELOW THIS LINE
def requires_cache(fields):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            id = request.args.get('id')

            if id is None:
                return jsonify({"type": "error", "error": "No id provided"})
            
            for field in fields:
                if cache.get(id=id, field=field) is None:
                    return jsonify({"type": "error", "error": f"No {field} found"})
            
            field_values = {field: cache.get(id=id, field=field) for field in fields}
            
            # Add the id to the field_values
            field_values['id'] = id

            return f(*args, **field_values, **kwargs)
        return decorated
    return decorator

@app.route('/api/v0/get_all_questions', methods=['GET'])
def get_all_questions():
    try:
        return db.fetch_all('SELECT * FROM question_data')
    except Exception as e:
        # Catch all other exceptions
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    
@app.route('/api/v0/create_question', methods=['POST'])
def create_question():
    try:
        training_id = request.form.get('training_id')
        training_data_type = request.form.get('training_data_type')
        question = request.form.get('question')
        content = request.form.get('content')
        db.execute_query("TRUNCATE TABLE question_data")
        db.execute_query("INSERT INTO question_data (training_id,training_data_type,question,content) VALUES (%s, %s, %s, %s)",(training_id, training_data_type,question,content))
        return jsonify(status='OK',message='Successfully created')
    except Exception as e:
        # Catch all other exceptions
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

@app.route('/api/v0/update_question', methods=['POST'])
def update_question():
   return db.fetch_all('SELECT * FROM question_data')

@app.route('/api/v0/replace_question_data', methods=['GET'])
def replace_question_data():
    data_list = []
    df = vn.get_training_data()
    db.execute_query('TRUNCATE TABLE question_data')
    for data in df.to_dict(orient='records'):
        content = data['content'].replace(r'\"', '`')
        question = ''
        if not data['training_data_type'] == 'ddl':
            question = data['question']
        
        db.execute_query("INSERT INTO question_data (training_id,training_data_type,question,content) VALUES (%s, %s, %s, %s)",(data['id'], data['training_data_type'],question,content))

    return jsonify(status='OK',message='Successfully replaced data', data = data_list)

@app.route('/api/v0/generate_questions', methods=['GET'])
def generate_questions():
    return jsonify({
        "type": "question_list", 
        "questions": vn.generate_questions(),
        "header": "Here are some questions you can ask:"
    })

@app.route('/api/v0/generate_sql', methods=['GET'])
def generate_sql():
    question = flask.request.args.get('question')

    if question is None:
        return jsonify({"type": "error", "error": "No question provided"})

    id = cache.generate_id(question=question)
    sql = vn.generate_sql(question=question, allow_llm_to_see_data=True)

    cache.set(id=id, field='question', value=question)
    cache.set(id=id, field='sql', value=sql)
    valid = vn.is_sql_valid(sql)
    if not valid:
        # my_prompt = [
        #     vn.system_message("You are a helpful assistant that will answer queries about Malaysia and the connected database"),
        #     vn.user_message("Query: " + question),
        # ]
        # sql = vn.submit_prompt(prompt=my_prompt)
        slist = vn.get_related_documentation(question)
        sql = slist[0]
    return jsonify(
        {
            "type": "sql", 
            "id": id,
            "text": sql,
        })

@app.route('/api/v0/run_sql', methods=['GET'])
@requires_cache(['sql'])
def run_sql(id: str, sql: str):
    try:
        df = vn.run_sql(sql=sql)

        cache.set(id=id, field='df', value=df)

        return jsonify(
            {
                "type": "df", 
                "id": id,
                "df": df.to_json(orient='records'),
            })

    except Exception as e:
        return jsonify({"type": "error", "error": str(e)})

@app.route('/api/v0/run_sql', methods=['POST'])
def run_sql_post():
    data = request.get_json()
    sql = data.get("sql") if data else None
    print("sql", sql)
    if sql is None:
        return jsonify({"type": "error", "error": "No SQL query provided", "sql": sql})
    try:
        df = vn.run_sql(sql=sql)
        return jsonify({"type": "df", "df": df.to_json(orient="records")})
    except Exception as e:
        return jsonify({"type": "error", "error": str(e)})

@app.route('/api/v0/download_csv', methods=['GET'])
@requires_cache(['df'])
def download_csv(id: str, df):
    csv = df.to_csv()

    return Response(
        csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                 f"attachment; filename={id}.csv"})

@app.route('/api/v0/generate_plotly_figure', methods=['GET'])
@requires_cache(['df', 'question', 'sql'])
def generate_plotly_figure(id: str, df, question, sql):
    try:
        code = vn.generate_plotly_code(question=question, sql=sql, df_metadata=f"Running df.dtypes gives:\n {df.dtypes}")
        fig = vn.get_plotly_figure(plotly_code=code, df=df, dark_mode=True)
        fig_json = fig.to_json()

        cache.set(id=id, field='fig_json', value=fig_json)

        return jsonify(
            {
                "type": "plotly_figure", 
                "id": id,
                "fig": fig_json,
            })
    except Exception as e:
        # Print the stack trace
        import traceback
        traceback.print_exc()

        return jsonify({"type": "error", "error": str(e)})

@app.route('/api/v0/get_training_data', methods=['GET'])
def get_training_data():
    df = vn.get_training_data()

    return jsonify(
    {
        "type": "df", 
        "id": "training_data",
        "df": df.to_json(orient='records'),
    })
@app.route('/api/v0/remove_training_data', methods=['POST'])
def remove_training_data():
    # Get id from the JSON body
    id = flask.request.json.get('id')

    if id is None:
        return jsonify({"type": "error", "error": "No id provided"})

    if vn.remove_training_data(id=id):
        return jsonify({"success": True})
    else:
        return jsonify({"type": "error", "error": "Couldn't remove training data"})

@app.route('/api/v0/train', methods=['POST'])
def add_training_data():
    question = flask.request.json.get('question')
    sql = flask.request.json.get('sql')
    ddl = flask.request.json.get('ddl')
    documentation = flask.request.json.get('documentation')

    try:
        id = vn.train(question=question, sql=sql, ddl=ddl, documentation=documentation)

        return jsonify({"id": id})
    except Exception as e:
        print("TRAINING ERROR", e)
        return jsonify({"type": "error", "error": str(e)})

@app.route('/api/v0/trainplan', methods=['POST'])
def add_training_data_plan():
    df_information_schema = vn.run_sql("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")
    plan = vn.get_training_plan_generic(df_information_schema)
    try:
        id = vn.train(plan=plan)
        return jsonify({"id": id})
    except Exception as e:
        print("TRAINING ERROR", e)
        return jsonify({"type": "error", "error": str(e)})

@app.route('/api/v0/generate_followup_questions', methods=['GET'])
@requires_cache(['df', 'question', 'sql'])
def generate_followup_questions(id: str, df, question, sql):
    followup_questions = vn.generate_followup_questions(question=question, sql=sql, df=df)

    cache.set(id=id, field='followup_questions', value=followup_questions)

    return jsonify(
        {
            "type": "question_list", 
            "id": id,
            "questions": followup_questions,
            "header": "Here are some followup questions you can ask:"
        })

@app.route('/api/v0/load_question', methods=['GET'])
@requires_cache(['question', 'sql', 'df', 'fig_json', 'followup_questions'])
def load_question(id: str, question, sql, df, fig_json, followup_questions):
    try:
        return jsonify(
            {
                "type": "question_cache", 
                "id": id,
                "question": question,
                "sql": sql,
                "df": df.head(10).to_json(orient='records'),
                "fig": fig_json,
                "followup_questions": followup_questions,
            })

    except Exception as e:
        return jsonify({"type": "error", "error": str(e)})

@app.route('/api/v0/get_question_history', methods=['GET'])
def get_question_history():
    return jsonify({"type": "question_history", "questions": cache.get_all(field_list=['question']) })

@app.route('/')
def root():
    return app.send_static_file('index.html')

if __name__ == '__main__':
    app = VannaFlaskApp(vn, allow_llm_to_see_data=True)
    app.run(debug=True)
