import base64
import email
import imaplib
import os.path
import pickle
import signal
import time
from pathlib import Path
from datetime import datetime, timedelta
import json
import logging
from shutil import copyfile

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import mysql.connector
import msal
import pdfkit
import requests
from dateutil.parser import parse
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pytz import timezone
from email.header import decode_header


from make_log import log_exceptions, custom_log_data
from settings import mail_time, file_no, file_blacklist, conn_data, pdfconfig, format_date, save_attachment, \
    hospital_data, interval, clean_filename


class TimeOutException(Exception):
    pass


def alarm_handler(signum, frame):
    print("ALARM signal received")
    raise TimeOutException()
# all_mails_fields = ("id","subject","date","sys_time","attach_path","completed","sender","hospital","insurer","process","deferred")

def failed_mails(mid, date, subject, hospital, folder):
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q1 = "select * from failed_storage_mails where `id`=%s and subject=%s and `date`=%s limit 1"
        data1 = (mid, subject, date)
        cur.execute(q1, data1)
        result = cur.fetchone()
        if result is None:
            q = "insert into failed_storage_mails (`id`,`subject`,`date`,`sys_time`,`hospital`,`folder`, `sender`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            data = (mid, subject, date, str(datetime.now()), hospital, folder, '')
            cur.execute(q, data)
            con.commit()

def create_settlement_folder(hosp, ins, date, filepath):
    try:
        date = datetime.strptime(date, '%d/%m/%Y %H:%M:%S').strftime('%m%d%Y%H%M%S')
        folder = os.path.join(hosp, "letters", f"{ins}_{date}")
        dst = os.path.join(folder, os.path.split(filepath)[-1])
        Path(folder).mkdir(parents=True, exist_ok=True)
        copyfile(filepath, dst)
    except:
        log_exceptions(hosp=hosp, ins=ins, date=date, filepath=filepath)

def get_ins_process(subject, email):
    ins, process = "", ""
    q1 = "select IC from email_ids where email_ids=%s"
    q2 = "select subject, table_name from email_master where ic_id=%s"
    q3 = "select IC_name from IC_name where IC=%s"
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q1, (email,))
        result =cur.fetchone()
        if result is not None:
            ic_id = result[0]
            cur.execute(q2, (ic_id,))
            result = cur.fetchall()
            for sub, pro in result:
                if 'Intimation No' in subject:
                    return ('big', 'settlement')
                if 'STAR HEALTH AND ALLIED INSUR04239' in subject:
                    return ('small', 'settlement')
                if sub in subject:
                    cur.execute(q3, (ic_id,))
                    result1 = cur.fetchone()
                    if result1 is not None:
                        return (result1[0], pro)
    return ins, process

def get_folders(hospital, deferred):
    result = []
    if deferred == 'X':
        q = "select historical from mail_folder_config where hospital=%s"
    else:
        q = "select current from mail_folder_config where hospital=%s and current != ''"
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q, (hospital,))
        records = cur.fetchall()
        result = [i[0] for i in records]
    return result

def if_exists(**kwargs):
    q = f"select * from {kwargs['hosp']}_mails where subject=%s and date=%s and id=%s limit 1"
    data = (kwargs['subject'], kwargs['date'], kwargs['id'])
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q, data)
        result = cur.fetchone()
        if result is not None:
            return True
    return False

def gmail_api(data, hosp, deferred, mid):
    try:
        print(hosp)
        after = datetime.now() - timedelta(minutes=mail_time)
        after = int(after.timestamp())
        attach_path = os.path.join(hosp, 'new_attach/')
        token_file = data['data']['token_file']
        cred_file = data['data']['json_file']
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
        creds = None
        if os.path.exists(token_file):
            with open(token_file, 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    cred_file, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_file, 'wb') as token:
                pickle.dump(creds, token)
        service = build('gmail', 'v1', credentials=creds, cache_discovery=False)
        for folder in get_folders(hosp, deferred):
            with open('logs/folders.log', 'a') as tfp:
                print(str(datetime.now()), hosp, folder, sep=',', file=tfp)
            q = f"after:{str(after)}"
            results = service.users().messages()
            msg = results.get(userId='me', id=mid).execute()
            if msg is not None:
                id = msg['id']
                for i in msg['payload']['headers']:
                    if i['name'] == 'Subject':
                        subject = i['value']
                    if i['name'] == 'From':
                        sender = i['value']
                        sender = sender.split('<')[-1].replace('>', '')
                    if i['name'] == 'Date':
                        date = i['value']
                        date = date.split(',')[-1].strip()
                        format = '%d %b %Y %H:%M:%S %z'
                        if '(' in date:
                            date = date.split('(')[0].strip()
                        try:
                            date = datetime.strptime(date, format)
                        except:
                            try:
                                date = parse(date)
                            except:
                                with open('logs/date_err.log', 'a') as fp:
                                    print(date, file=fp)
                                raise Exception
                        date = date.astimezone(timezone('Asia/Kolkata')).replace(tzinfo=None)
                        format1 = '%d/%m/%Y %H:%M:%S'
                        date = date.strftime(format1)
                # if if_exists(id=id, subject=subject, date=date, hosp=hosp):
                #     continue
                mail_attach_filepath = ''
                try:
                    flag = 0
                    if 'parts' in msg['payload']:
                        for j in msg['payload']['parts']:
                            if 'attachmentId' in j['body']:
                                temp = j['filename']
                                if file_blacklist(temp, email=sender):
                                    filename = clean_filename(temp)
                                    filename = attach_path + file_no(4) + filename
                                    a_id = j['body']['attachmentId']
                                    attachment = service.users().messages().attachments().get(userId='me', messageId=id,
                                                                                              id=a_id).execute()
                                    data = attachment['data']
                                    with open(filename, 'wb') as fp:
                                        fp.write(base64.urlsafe_b64decode(data))
                                    print(filename)
                                    flag = 1
                        if flag == 0:
                            for j in msg['payload']['parts']:
                                if j['filename'] == '':
                                    data = j['body']['data']
                                    filename = attach_path + file_no(8) + '.pdf'
                                    with open(attach_path + 'temp.html', 'wb') as fp:
                                        fp.write(base64.urlsafe_b64decode(data))
                                    print(filename)
                                    pdfkit.from_file(attach_path + 'temp.html', filename, configuration=pdfconfig)
                                    flag = 1
                    else:
                        data = msg['payload']['body']['data']
                        filename = attach_path + file_no(8) + '.pdf'
                        with open(attach_path + 'temp.html', 'wb') as fp:
                            fp.write(base64.urlsafe_b64decode(data))
                        print(filename)
                        pdfkit.from_file(attach_path + 'temp.html', filename, configuration=pdfconfig)
                        flag = 1
                    if flag == 0:
                        if 'data' in msg['payload']['parts'][-1]['body']:
                            data = msg['payload']['parts'][-1]['body']['data']
                            filename = attach_path + file_no(8) + '.pdf'
                            with open(attach_path + 'temp.html', 'wb') as fp:
                                fp.write(base64.urlsafe_b64decode(data))
                            print(filename)
                            pdfkit.from_file(attach_path + 'temp.html', filename, configuration=pdfconfig)
                            flag = 1
                        else:
                            if 'data' in msg['payload']['parts'][0]['parts'][-1]['body']:
                                data = msg['payload']['parts'][0]['parts'][-1]['body']['data']
                                filename = attach_path + file_no(8) + '.pdf'
                                with open(attach_path + 'temp.html', 'wb') as fp:
                                    fp.write(base64.urlsafe_b64decode(data))
                                print(filename)
                                pdfkit.from_file(attach_path + 'temp.html', filename, configuration=pdfconfig)
                                flag = 1
                            else:
                                data = msg['payload']['parts'][0]['parts'][-1]['parts'][-1]['body']['data']
                                filename = attach_path + file_no(8) + '.pdf'
                                with open(attach_path + 'temp.html', 'wb') as fp:
                                    fp.write(base64.urlsafe_b64decode(data))
                                print(filename)
                                pdfkit.from_file(attach_path + 'temp.html', filename, configuration=pdfconfig)
                                flag = 1
                    mail_attach_filepath = filename
                    if mail_attach_filepath != '':
                        directory = f"../{hosp}/new_attach"
                        Path(directory).mkdir(parents=True, exist_ok=True)
                        dst = os.path.join(directory, os.path.split(mail_attach_filepath)[-1])
                        os.replace(mail_attach_filepath, dst)
                        mail_attach_filepath = os.path.abspath(dst)
                except:
                    log_exceptions(id=id, hosp=hosp, folder=folder)
                with mysql.connector.connect(**conn_data) as con:
                    cur = con.cursor()
                    q = f"insert into {hosp}_mails (`id`,`subject`,`date`,`sys_time`,`attach_path`,`completed`, `sender`, `folder`, `process`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    data = (id, subject, date, str(datetime.now()), mail_attach_filepath, '', sender, folder, 'main')
                    cur.execute(q, data)
                    # con.commit()
    except:
        log_exceptions()

def graph_api(data, hosp, deferred, mid):
    try:
        print(hosp)
        folder = ""
        attachfile_path = os.path.join(hosp, 'new_attach/')
        email = data['data']['email']
        cred_file = data['data']['json_file']
        config = json.load(open(cred_file))
        app = msal.ConfidentialClientApplication(
            config["client_id"], authority=config["authority"],
            client_credential=config["secret"], )
        result = None
        result = app.acquire_token_silent(config["scope"], account=None)
        if not result:
            logging.info("No suitable token exists in cache. Let's get a new one from AAD.")
            result = app.acquire_token_for_client(scopes=config["scope"])
        if "access_token" in result:
            query = f"https://graph.microsoft.com/v1.0/users/{email}/messages/{mid}"
            i = requests.get(query, headers={'Authorization': 'Bearer ' + result['access_token']}, ).json()
            try:
                date, subject, attach_path, sender = '', '', '', ''
                format = "%Y-%m-%dT%H:%M:%SZ"
                b = datetime.strptime(i['receivedDateTime'], format).replace(tzinfo=pytz.utc).astimezone(
                    pytz.timezone('Asia/Kolkata')).replace(
                    tzinfo=None)
                b = b.strftime('%d/%m/%Y %H:%M:%S')
                date, subject, sender = b, i['subject'], i['sender']['emailAddress']['address']
                mail_attach_filepath = ''
                flag = 0
                try:
                    if i['hasAttachments'] is True:
                        q = f"https://graph.microsoft.com/v1.0/users/{email}/mailFolders/inbox/messages/{i['id']}/attachments"
                        attach_data = requests.get(q,
                                                   headers={'Authorization': 'Bearer ' + result[
                                                       'access_token']}, ).json()
                        for j in attach_data['value']:
                            if '@odata.mediaContentType' in j:
                                j['name'] = j['name'].replace('.PDF', '.pdf')
                                # print(j['@odata.mediaContentType'], j['name'])
                                if file_blacklist(j['name'], email=sender):
                                    j['name'] = file_no(4) + j['name']
                                    with open(os.path.join(attachfile_path, j['name']), 'w+b') as fp:
                                        fp.write(base64.b64decode(j['contentBytes']))
                                    attach_path = os.path.join(attachfile_path, j['name'])
                                    flag = 1
                    if flag == 0:
                        filename = attachfile_path + file_no(8) + '.pdf'
                        if i['body']['contentType'] == 'html':
                            with open(attachfile_path + 'temp.html', 'w') as fp:
                                fp.write(i['body']['content'])
                            pdfkit.from_file(attachfile_path +'temp.html', filename, configuration=pdfconfig)
                            attach_path = filename
                        elif i['body']['contentType'] == 'text':
                            with open(attachfile_path + 'temp.text', 'w') as fp:
                                fp.write(i['body']['content'])
                            pdfkit.from_file(attachfile_path + 'temp.text', filename, configuration=pdfconfig)
                            attach_path = filename
                    mail_attach_filepath = attach_path
                    if mail_attach_filepath != '':
                        directory = f"../{hosp}/new_attach"
                        Path(directory).mkdir(parents=True, exist_ok=True)
                        dst = os.path.join(directory, os.path.split(mail_attach_filepath)[-1])
                        os.replace(mail_attach_filepath, dst)
                        mail_attach_filepath = os.path.abspath(dst)
                except:
                    log_exceptions(mid=i['id'], hosp=hosp, folder=folder)
                with mysql.connector.connect(**conn_data) as con:
                    cur = con.cursor()
                    q = f"insert into {hosp}_mails (`id`,`subject`,`date`,`sys_time`,`attach_path`,`completed`, `sender`, `folder`, `process`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    data = (
                    i['id'], subject, date, str(datetime.now()), mail_attach_filepath, '', sender, folder,
                    'main')
                    cur.execute(q, data)
                    con.commit()
            except:
                log_exceptions(mid=i['id'], hosp=hosp, folder=folder)
                failed_mails(i['id'], date, subject, hosp, folder)
    except:
        log_exceptions(hosp=hosp)

def imap_(data, hosp, deferred):
    try:
        print(hosp)
        after = datetime.now() - timedelta(minutes=mail_time)
        after = after.strftime('%d-%b-%Y')
        attachfile_path = os.path.join(hosp, 'new_attach/')
        server, email_id, password = data['data']['host'], data['data']['email'], data['data']['password']
        today = datetime.now().strftime('%d-%b-%Y')
        imap_server = imaplib.IMAP4_SSL(host=server)
        table = f'{hosp}_mails'
        imap_server.login(email_id, password)
        for folder in get_folders(hosp, deferred):
            with open('logs/folders.log', 'a') as tfp:
                print(str(datetime.now()), hosp, folder, sep=',', file=tfp)
            imap_server.select(readonly=True, mailbox=f'"{folder}"')  # Default is `INBOX`
            # Find all emails in inbox and print out the raw email data
            # _, message_numbers_raw = imap_server.search(None, 'ALL')
            _, message_numbers_raw = imap_server.search(None, f'(SINCE "{after}")')
            for message_number in message_numbers_raw[0].split():
                try:
                    _, msg = imap_server.fetch(message_number, '(RFC822)')
                    message = email.message_from_bytes(msg[0][1])
                    sender = message['from']
                    sender = sender.split('<')[-1].replace('>', '')
                    date = format_date(message['Date'])
                    subject = message['Subject'].strip()
                    if '?' in subject:
                        try:
                            subject = decode_header(subject)[0][0].decode("utf-8")
                        except:
                            log_exceptions(subject=subject, hosp=hosp)
                            pass
                    for i in ['\r', '\n', '\t']:
                        subject = subject.replace(i, '').strip()
                    mid = int(message_number)
                    if if_exists(id=mid, date=date, subject=subject, hosp=hosp):
                        continue
                    mail_attach_filepath = ""
                    try:
                        a = save_attachment(message, attachfile_path, email=sender)
                        if not isinstance(a, list):
                            filename = attachfile_path + file_no(8) + '.pdf'
                            pdfkit.from_file(a, filename, configuration=pdfconfig)
                        else:
                            filename = a[-1]
                        mail_attach_filepath = filename
                        if mail_attach_filepath != '':
                            directory = f"../{hosp}/new_attach"
                            Path(directory).mkdir(parents=True, exist_ok=True)
                            dst = os.path.join(directory, os.path.split(mail_attach_filepath)[-1])
                            os.replace(mail_attach_filepath, dst)
                            mail_attach_filepath = os.path.abspath(dst)
                    except:
                        log_exceptions(subject=subject, date=date, hosp=hosp, folder=folder)
                    with mysql.connector.connect(**conn_data) as con:
                        cur = con.cursor()
                        q = f"insert into {hosp}_mails (`id`,`subject`,`date`,`sys_time`,`attach_path`,`completed`, `sender`, `folder`, `process`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        data = (
                            mid, subject, date, str(datetime.now()), mail_attach_filepath, '', sender, folder,
                            'main')
                        cur.execute(q, data)
                        con.commit()
                except:
                    log_exceptions(subject=subject, date=date, hosp=hosp, folder=folder)
                    failed_mails(mid, date, subject, hosp, folder)
    except:
        log_exceptions(hosp=hosp)

def mail_mover(hospital, deferred):
    fields = ("id","subject","date","sys_time","attach_path","completed","sender","hospital","insurer","process","deferred","sno")
    q = "select * from all_mails where deferred=%s and hospital=%s"
    records = []
    folder = f"../{hospital}/new_attach"
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q, (deferred, hospital,))
        result = cur.fetchall()
        for i in result:
            temp = {}
            for key, value in zip(fields, i):
                temp[key] = value
            records.append(temp)
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        for i in records:
            dst = os.path.join(folder, os.path.split(i["attach_path"])[-1])
            Path(folder).mkdir(parents=True, exist_ok=True)
            copyfile(i["attach_path"], dst)
            q = f"INSERT INTO {hospital}_mails (`id`,`subject`,`date`,`sys_time`,`attach_path`,`completed`,`sender`) values (%s, %s, %s, %s, %s, %s, %s)"
            data = (i["id"], i["subject"], i["date"], str(datetime.now()), os.path.abspath(dst), i["completed"], i["sender"])
            cur.execute(q, data)
            q = "update all_mails set deferred='MOVED' where sno=%s"
            cur.execute(q, (i['sno'],))
            con.commit()

def mail_storage(hospital, deferred):
    for hosp, data in hospital_data.items():
        if data['mode'] == 'gmail_api' and hosp == hospital:
            print(hosp)
            gmail_api(data, hosp, deferred)
        elif data['mode'] == 'graph_api' and hosp == hospital:
            print(hosp) #.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            graph_api(data, hosp, deferred)
        elif data['mode'] == 'imap_' and hosp == hospital:
            print(hosp)
            imap_(data, hosp, deferred)

def mail_storage_job(hospital, deferred):
    sched = BackgroundScheduler(daemon=False)
    for hosp, data in hospital_data.items():
        if data['mode'] == 'gmail_api':
            sched.add_job(gmail_api, 'interval', seconds=interval, max_instances=1,
                          args=[data, hosp, deferred])
        elif data['mode'] == 'graph_api':
            sched.add_job(graph_api, 'interval', seconds=interval, max_instances=1,
                          args=[data, hosp, deferred])
        elif data['mode'] == 'imap_':
            sched.add_job(imap_, 'interval', seconds=interval, max_instances=1,
                          args=[data, hosp, deferred])
    sched.start()

if __name__ == '__main__':
    gmail_api(hospital_data['noble'], "noble", '', '178071eff982c505')
    pass