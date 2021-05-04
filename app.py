from datetime import datetime

from flask import Flask, request, jsonify, url_for, send_from_directory
from flask_cors import CORS

import mysql.connector

from settings import conn_data
from utr_search_backend import create_settlement_folder

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

@app.route("/getallmails", methods=["POST"])
def get_all_mails():
    temp_dict = []
    data = request.form.to_dict()
    fields = ["id","subject","date","sys_time","attach_path","completed","sender","hospital","insurer","process","deferred","sno","mail_folder"]
    link_text = request.url_root + 'download?path='
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = "select * from all_mails where subject like '%ettlement%' and  hospital=%s and process=''"
        cur.execute(q, (data['hospital'],))
        result1 = cur.fetchall()
        for i in result1:
            temp = {}
            for k, v in zip(fields, i):
                temp[k] = v
            temp['attach_path'] = link_text + temp['attach_path']
            temp_dict.append(temp)
        return jsonify(temp_dict)

@app.route("/moveinsett", methods=["POST"])
def move_in_sett():
    fields = ["id","subject","date","sys_time","attach_path","completed","sender","hospital","insurer","process","deferred","sno","mail_folder"]
    data = request.form.to_dict()
    temp = {}

    q = "update all_mails set "
    params = []
    for i in data:
        if i in fields and i != 'sno':
            q = q + f"`{i}`=%s, "
            params.append(data[i])
    q = q + "where `sno`=%s"
    params.append(data['sno'])
    q = q.replace(', where', ' where')

    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q, params)
        con.commit()

    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = "select * from all_mails where sno=%s limit 1"
        cur.execute(q, (data['sno'],))
        result1 = cur.fetchone()
        for k, v in zip(fields, result1):
            temp[k] = v
        file_path = create_settlement_folder(temp['hospital'], data['insurer'], temp['date'], temp['attach_path'])
        q = 'INSERT INTO settlement_mails (`id`,`subject`,`date`,`sys_time`,`attach_path`,`completed`,`sender`,`folder`,`process`,`hospital`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        data1 = (
        temp['id'], temp['subject'], temp['date'], str(datetime.now()), file_path, '', temp['sender'], '',
        'all_mails_set_api', temp['hospital'])
        cur.execute(q, data1)
        q = "update all_mails set completed='FIXED_INFO' where sno=%s"
        cur.execute(q, (data['sno'],))
        con.commit()
    return jsonify("done")

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
        q = "select utr from settlement_utrs where search_completed!='X'"
        cur.execute(q)
        result = cur.fetchall()
        temp = []
        temp = [{"utr": i[0]} for i in result]
        return jsonify(temp)

@app.route("/setutrflag", methods=["POST"])
def set_utr_flag():
    data = request.form.to_dict()
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = "update settlement_utrs set search_completed='X' where utr=%s and sno=%s"
        cur.execute(q, (data['utr'], data['sno']))
        con.commit()
    return jsonify({"msg": "done"})

@app.route("/setutrmails", methods=["POST"])
def set_utr_mails():
    #make settlement folder stucture
    fields = ("sno","hospital","utr","utr2","completed","sett_table_sno","id","subject","date","sys_time","attach_path","sender","folder")
    data = request.form.to_dict()
    with mysql.connector.connect(**conn_data) as con:
        # make fun for moving from utr_mails to utr_copy and delete
        cur = con.cursor()
        if 'utr' in data:
            q = "update utr_mails set completed='D' where utr=%s"
            cur.execute(q, (data['utr'],))
            con.commit()
        if 'sno' in data and 'insurer' not in data:
            q = "update utr_mails set completed='D' where sno=%s"
            cur.execute(q, (data['sno'],))
            con.commit()
            set_utr_mails_flag(data['sno'])
        if 'insurer' in data and 'sno' in data:
            if data['insurer'] == '_blank':
                q = "update utr_mails set completed='D' where sno=%s"
                cur.execute(q, (data['sno'],))
                con.commit()
            else:
                q = "select * from utr_mails where sno=%s limit 1"
                cur.execute(q, (data['sno'],))
                r = cur.fetchone()
                if r is not None:
                    temp = {}
                    for k, v in zip(fields, r):
                        temp[k] = v
                    file_path = create_settlement_folder(temp['hospital'], data['insurer'], temp['date'], temp['attach_path'])
                    q = 'INSERT INTO settlement_mails (`id`,`subject`,`date`,`sys_time`,`attach_path`,`completed`,`sender`,`folder`,`process`,`hospital`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
                    data1 = (temp['id'], temp['subject'], temp['date'], str(datetime.now()), file_path, '', temp['sender'], temp['folder'], 'utr_mails', temp['hospital'])
                    cur.execute(q, data1)
                    con.commit()
                    q = 'select sno from settlement_mails where id=%s and subject=%s and date=%s limit 1;'
                    data1 = (temp['id'], temp['subject'], temp['date'])
                    cur.execute(q, data1)
                    sett_sno = cur.fetchone()[0]
                    q = "update utr_mails set completed='MOVED', insurer=%s, sett_table_sno=%s where sno=%s"
                    cur.execute(q, (data['insurer'], sett_sno, data['sno']))
                    con.commit()
    return jsonify({"msg": "done"})

@app.route("/getutrbreakup", methods=["POST"])
def get_utr_breakup():
    records = []
    fields = ("utrNo","insRefNo","policyNo","claimNo","patientName","grossAmount","tds","netAmount","tpaNo","hospital")
    data = request.form.to_dict()
    q = "select City_Records.City_Transaction_Reference,NIC_Records.Transaction_Reference_No,NIC_Records.Policy_Number,NIC_Records.Claim_Number,NIC_Records.Name_Of_Patient,NIC_Records.Gross_Amounts,NIC_Records.tds,NIC_Records.Net_Amount,NIC_Records.tpa_No,NIC_Records.hospital from NIC_Records inner join City_Records where City_Records.City_Transaction_Reference=%s and City_Records.NIA_Transaction_Reference=NIC_Records.Transaction_Reference_No"
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q, (data['utrNo'],))
        result = cur.fetchall()
        for row in result:
            temp = {}
            for k, v in zip(fields, row):
                temp[k] = v
            records.append(temp)
    return jsonify(records)


def set_utr_mails_flag(sno):
    q = "INSERT INTO utr_mails_copy SELECT * FROM utr_mails WHERE sno=%s; DELETE FROM utr_mails WHERE sno=%s;"
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        for i in q.split(';'):
            cur.execute(i, (sno,))
        con.commit()
        q = "select utr from utr_mails_copy where sno=%s limit 1"
        cur.execute(q, (sno,))
        r = cur.fetchone()
        if r is not None:
            utr = r[0]
            q = "select * from utr_mails where utr=%s"
            cur.execute(q, (utr,))
            r1 = cur.fetchall()
            for i in r1:
                q = "update settlement_utrs set search_completed='X' where utr=%s"
                cur.execute(q, (utr,))
                con.commit()
                break

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9984)
