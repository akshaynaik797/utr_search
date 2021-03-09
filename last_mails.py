import email
import email
import imaplib
import json
import logging
import os.path
import pickle
from datetime import datetime, timedelta
from email.header import decode_header
from pathlib import Path
from shutil import copyfile

import msal
import mysql.connector
import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil.parser import parse
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pytz import timezone

from make_log import log_exceptions, custom_log_data
from settings import conn_data, format_date, hospital_data, interval

mail_time = 600


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

def gmail_api(data, hosp, deferred):
    last_mails = []
    connection = ""
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
            date_list, sub_list = [], []
            q = f"after:{str(after)}"
            results = service.users().messages()
            request = results.list(userId='me', labelIds=[folder], q=q)
            while request is not None:
                msg_col = request.execute()
                messages = msg_col.get('messages', [])
                custom_log_data(filename=hosp+'_mails', data=messages)
                if not messages:
                    pass
                    #print("No messages found.")
                else:
                    connection = "X"
                    print("Message snippets:")
                    if len(messages) > 0:
                        for message in messages:
                            try:
                                id, subject, date, filename, sender = '', '', '', '', ''
                                msg = service.users().messages().get(userId='me', id=message['id']).execute()
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
                                        temp_date = date = date.astimezone(timezone('Asia/Kolkata')).replace(tzinfo=None)
                                        format1 = '%d/%m/%Y %H:%M:%S'
                                        date = date.strftime(format1)
                                date_list.append(temp_date)
                                sub_list.append(subject)
                            except:
                                log_exceptions(id=id, hosp=hosp, folder=folder)
                request = results.list_next(request, msg_col)
            format1 = '%d/%m/%Y %H:%M:%S'
            date1 = max(date_list).strftime(format1)
            subject = sub_list[date_list.index(max(date_list))]
            last_mails.append({"hosp":hosp, "folder":folder, "subject":subject, "date":date1, 'connection':connection})
    except:
        log_exceptions(hosp=hosp)
        last_mails.append({"connection":connection})
    finally:
        return last_mails

def graph_api(data, hosp, deferred):
    last_mails = []
    connection = ""
    try:
        print(hosp)
        after = datetime.now() - timedelta(minutes=mail_time)
        after = after.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
        after = datetime.now() - timedelta(minutes=mail_time)
        after = after.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if "access_token" in result:
            for folder in get_folders(hosp, deferred):
                # with open('logs/folders.log', 'a') as tfp:
                #     print(str(datetime.now()), hosp, folder, sep=',', file=tfp)
                flag = 0
                while 1:
                    if flag == 0:
                        query = f"https://graph.microsoft.com/v1.0/users/{email}" \
                                f"/mailFolders/{folder}/messages?$filter=(receivedDateTime ge {after})"
                    flag = 1
                    graph_data2 = requests.get(query,
                                               headers={'Authorization': 'Bearer ' + result['access_token']}, ).json()
                    if 'value' in graph_data2:
                        connection = "X"
                        if len(graph_data2['value']) > 0:
                            for i in graph_data2['value']:
                                try:
                                    date, subject, attach_path, sender = '', '', '', ''
                                    format = "%Y-%m-%dT%H:%M:%SZ"
                                    b = datetime.strptime(i['receivedDateTime'], format).replace(tzinfo=pytz.utc).astimezone(
                                        pytz.timezone('Asia/Kolkata')).replace(
                                        tzinfo=None)
                                    b = b.strftime('%d/%m/%Y %H:%M:%S')
                                    date, subject, sender = b, i['subject'], i['sender']['emailAddress']['address']
                                except:
                                    log_exceptions(mid=i['id'], hosp=hosp, folder=folder)
                    else:
                        with open('logs/query.log', 'a') as fp:
                            print(query, file=fp)
                    if '@odata.nextLink' in graph_data2:
                        query = graph_data2['@odata.nextLink']
                    else:
                        break
                last_mails.append({"hosp": hosp, "folder": folder, "subject": subject, "date": date, 'connection':connection})
    except:
        log_exceptions(hosp=hosp)
        last_mails.append({"connection":connection})
    finally:
        return last_mails

def imap_(data, hosp, deferred):
    last_mails = []
    connection = ""
    try:
        print(hosp)
        after = datetime.now()
        after = after.strftime('%d-%b-%Y')
        attachfile_path = os.path.join(hosp, 'new_attach/')
        server, email_id, password = data['data']['host'], data['data']['email'], data['data']['password']
        today = datetime.now().strftime('%d-%b-%Y')
        imap_server = imaplib.IMAP4_SSL(host=server)
        table = f'{hosp}_mails'
        imap_server.login(email_id, password)
        for folder in get_folders(hosp, deferred):
            # with open('logs/folders.log', 'a') as tfp:
            #     print(str(datetime.now()), hosp, folder, sep=',', file=tfp)
            imap_server.select(readonly=True, mailbox=f'"{folder}"')  # Default is `INBOX`
            connection = "X"
            # Find all emails in inbox and print out the raw email data
            # _, message_numbers_raw = imap_server.search(None, 'ALL')
            _, message_numbers_raw = imap_server.search(None, f'(SINCE "{after}")')
            for message_number in [message_numbers_raw[0].split()[-1]]:
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
                except:
                    log_exceptions(subject=subject, date=date, hosp=hosp, folder=folder)
            last_mails.append({"hosp": hosp, "folder": folder, "subject": subject, "date": date, 'connection':connection})
    except:
        log_exceptions(hosp=hosp)
        last_mails.append({"connection":connection})
    finally:
        return last_mails


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

def last_mails_function(hospital, deferred):
    for hosp, data in hospital_data.items():
        if data['mode'] == 'gmail_api' and hosp == hospital:
            print(hosp)
            return gmail_api(data, hosp, deferred)
        elif data['mode'] == 'graph_api' and hosp == hospital:
            print(hosp) #.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            return graph_api(data, hosp, deferred)
        elif data['mode'] == 'imap_' and hosp == hospital:
            print(hosp)
            return imap_(data, hosp, deferred)

def mail_storage_job(hospital, deferred):
    sched = BackgroundScheduler(daemon=False)
    for hosp, data in hospital_data.items():
        if data['mode'] == 'gmail_api':
            print(hosp)
            sched.add_job(gmail_api, 'interval', seconds=interval, max_instances=1,
                          args=[data, hosp, deferred])
            # gmail_api(data, hosp, deferred)
        elif data['mode'] == 'graph_api':
            print(hosp) #.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            sched.add_job(graph_api, 'interval', seconds=interval, max_instances=1,
                          args=[data, hosp, deferred])
            # graph_api(data, hosp, deferred)
        elif data['mode'] == 'imap_':
            print(hosp)
            sched.add_job(imap_, 'interval', seconds=interval, max_instances=1,
                          args=[data, hosp, deferred])
            # imap_(data, hosp, deferred)
    sched.start()

if __name__ == '__main__':
    a = get_ins_process('STAR HEALTH AND ALLIED INSUR04239 - 00040350005154', 'Enetadvicemailing@hdfcbank.net')
    pass