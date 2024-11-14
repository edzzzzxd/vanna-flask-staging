import pymysql
import os
from dotenv import load_dotenv
load_dotenv()

class Database:


    def __init__(self):
        host = os.getenv('DB_HOST')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        db = os.getenv('DB_DATABASE')
        port = int(os.getenv('DB_PORT'))

        
        self.con = pymysql.connect(host=host, port = port, user=user, password=password, db=db, cursorclass=pymysql.cursors.DictCursor)
        self.con.autocommit(True)
        self.cur = self.con.cursor()

    def fetch_all(self,query):
        self.cur.execute(query)
        return self.cur.fetchall()

    def fetch_one(self, query):
        self.cur.execute(query)
        return self.cur.fetchone()

    def execute_query(self, query, parameter = ()):
        return self.cur.execute(query, parameter)