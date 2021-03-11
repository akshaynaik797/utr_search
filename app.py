from flask import Flask, request, jsonify, url_for, send_from_directory
from flask_cors import CORS

import mysql.connector

from settings import conn_data

app = Flask(__name__)
cors = CORS(app)


app.config['CORS_HEADERS'] = 'Content-Type'
app.config['referrer_url'] = None


@app.route("/")
def index():
    return url_for('index', _external=True)

@app.route("/download")
def download():
    import os
    aa = request.url_root
    path = request.args.get('path')
    folder, file = os.path.split(path)
    return send_from_directory(folder, filename=file, as_attachment=True)

@app.route("/getutrmails", methods=["POST"])
def get_utr_mails():
    #add code for single utr
    link_text = request.url_root + 'download?path='
    temp_dict = []
    data = request.form.to_dict()
    fields = ("sno","hospital","utr","utr2","completed","sett_table_sno","id","subject","date","sys_time","attach_path","sender","folder")
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        if 'utr' in data:
            utr = data['utr']
            q = "select * from utr_mails where utr=%s and completed=''"
            cur.execute(q, (utr,))
            result1 = cur.fetchall()
            for i in result1:
                temp = {}
                for k, v in zip(fields, i):
                    temp[k] = v
                temp['attach_path'] = link_text + temp['attach_path']
                temp_dict.append(temp)
            return jsonify(temp_dict)

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
                temp['attach_path'] = link_text + temp['attach_path']
                temp_dict[utr].append(temp)
    return temp_dict

@app.route("/getutrs", methods=["POST"])
def get_utrs():
    data = request.form.to_dict()
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = "select utr from settlement_utrs where search_completed!=''"
        cur.execute(q)
        result = cur.fetchall()
        temp = [i[0] for i in result]
        return jsonify(temp)
    return []

@app.route("/setutrflag", methods=["POST"])
def set_utr_flag():
    data = request.form.to_dict()
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = "update settlement_utrs set search_completed='X' where utr=%s"
        cur.execute(q, (data['utr'],))
        con.commit()
    return jsonify({"msg": "done"})


@app.route("/setutrmails", methods=["POST"])
def set_utr_mails():
    data = request.form.to_dict()
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = "update utr_mails set completed='X' where sno=%s"
        cur.execute(q, (data['sno'],))
        con.commit()
    return jsonify({"msg": "done"})

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9984)
