from flask import Flask, request, jsonify, url_for
from flask_cors import CORS

import mysql.connector
from apscheduler.schedulers.background import BackgroundScheduler

from last_mails import last_mails_function
from settings import conn_data

app = Flask(__name__)
cors = CORS(app)


app.config['CORS_HEADERS'] = 'Content-Type'
app.config['referrer_url'] = None


@app.route("/")
def index():
    return url_for('index', _external=True)

@app.route("/getutrmails", methods=["POST"])
def get_utr_mails():
    temp_dict = {}
    data = request.form.to_dict()
    fields = ("sno","hospital","utr","utr2","completed","sett_table_sno","id","subject","date","sys_time","attach_path","sender","folder")
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = "select distinct(utr) from utr_mails where completed=''"
        cur.execute(q)
        result = cur.fetchall()
        for i in result:
            utr = i[0]
            q = "select * from utr_mails where utr=%s and completed=''"
            cur.execute(q, (utr,))
            result1 = cur.fetchall()
            temp_dict[utr] = []
            for i in result1:
                temp = {}
                for k, v in zip(fields, i):
                    temp[k] = v
                temp_dict[utr].append(temp)
    return temp_dict

@app.route("/setutrmails", methods=["POST"])
def set_utr_mails():
    data = request.form.to_dict()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9984)
