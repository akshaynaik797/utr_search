import base64
import email
import imaplib
import os.path
import pickle
import re
import signal
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
    hospital_data, interval, clean_filename, time_out, gen_dict_extract


class TimeOutException(Exception):
    pass

def insert_utr_mails_sett_mails(utr, utr2, sett_sno, id, subject, date, filepath, sender, hosp, folder):
    completed = ''
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = "select * from utr_mails where utr=%s and utr2=%s and sett_table_sno=%s and id=%s and subject=%s and date=%s"
        data = (utr, utr2, sett_sno, id, subject, date)
        cur.execute(q, data)
        result = cur.fetchone()
        if result is None:
            insurer, process = get_ins(subject, sender, date)
            if process == 'settlement' and sett_sno == '' and insurer != 'MULTIPLE':
                #code to insert in sett
                q = 'INSERT INTO settlement_mails (`id`,`subject`,`date`,`sys_time`,`attach_path`,`completed`,`sender`,`folder`,`process`,`hospital`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
                data = (id, subject, date, str(datetime.now()), filepath, '', sender, folder, 'utr_mails', hosp)
                cur.execute(q, data)
                con.commit()
                completed = 'MOVED'
                q = "update settlement_utrs set search_completed=%s where utr=%s"
                cur.execute(q, (completed, utr))
                con.commit()
                data = (id, subject, date)
                q = "select sno from settlement_mails where id=%s and subject=%s and date=%s limit 1"
                cur.execute(q, data)
                result = cur.fetchone()
                if result is not None:
                    sett_sno = result[0]
            q = 'INSERT INTO `utr_mails` (`hospital`,`utr`,`utr2`,`completed`,`sett_table_sno`,`id`,`subject`,`date`,`sys_time`,`attach_path`,`sender`,`folder`,`insurer`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
            data = (hosp, utr, utr2, completed, sett_sno, id, subject, date, str(datetime.now()), filepath, sender, folder, insurer)
            cur.execute(q, data)
            con.commit()

def get_from_settlement(mid, subject, date):
    data = (mid, subject, date)
    q = "select sno, attach_path from settlement_mails where id=%s and subject=%s and date=%s limit 1"
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q, data)
        result = cur.fetchone()
        if result is not None:
            return list(result)


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
    q1 = "select IC from email_ids where email_ids=%s limit 1"
    q2 = "select subject, table_name from email_master where ic_id=%s"
    q3 = "select IC_name from IC_name where IC=%s limit 1"
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor(buffered=True)
        cur.execute(q1, (email,))
        result = cur.fetchone()
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

def get_ins(subject, email, date):
    ins, pro = '', ''
    ins, pro = get_ins_process(subject, email)
    if ins != '' and pro != '' and pro == 'settlement':
        return ins, pro
    else:
        with mysql.connector.connect(**conn_data) as con:
            cur = con.cursor()
            q = "select attach_path from settlement_mails where subject=%s and date=%s limit 1"
            cur.execute(q, (subject, date))
            r = cur.fetchone()
            if r is not None:
                temp = re.compile(r'(?<=letters\/)[a-zA-Z]+').search(r[0])
                if temp is not None:
                    ins, pro = temp.group(), 'settlement'
                    return ins, pro
    q = "SELECT IC_name.IC_name, email_master.table_name, email_master.subject FROM IC_name inner join email_master where email_master.IC_ID =IC_name.IC and email_master.subject != '' and email_master.table_name='settlement'"
    ins_process = []
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q)
        result = cur.fetchall()
        if result is not None:
            for ins, pro, sub in result:
                if 'Intimation No' in subject:
                    ins_process.append(('big', 'settlement'))
                if 'STAR HEALTH AND ALLIED INSUR04239' in subject:
                    ins_process.append(('small', 'settlement'))
                if sub in subject:
                    ins_process.append((ins, pro))
    if len(ins_process) > 0:
        return 'MULTIPLE', 'settlement'
    else:
        return '', ''


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

def gmail_api(data, hosp, deferred, utr, utr2):
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
            q = utr2
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
                    print("Message snippets:")
                    for message in messages[::-1]:
                        signal.signal(signal.SIGALRM, alarm_handler)
                        signal.alarm(time_out)
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
                                    date = date.astimezone(timezone('Asia/Kolkata')).replace(tzinfo=None)
                                    format1 = '%d/%m/%Y %H:%M:%S'
                                    date = date.strftime(format1)
                            mail_attach_filepath, sett_sno = '', ''
                            temp = get_from_settlement(id, subject, date)
                            if temp is not None:
                                sett_sno, mail_attach_filepath = temp
                            if mail_attach_filepath == '':
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
                                                    try:
                                                        data = j['body']['data']
                                                    except KeyError:
                                                        data = gen_dict_extract('data', j)[-1]
                                                    filename = attach_path + file_no(8) + '.pdf'
                                                    with open(attach_path + 'temp.html', 'wb') as fp:
                                                        fp.write(base64.urlsafe_b64decode(data))
                                                    print(filename)
                                                    pdfkit.from_file(attach_path + 'temp.html', filename,
                                                                     configuration=pdfconfig)
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
                            insert_utr_mails_sett_mails(utr, utr2, sett_sno, id, subject, date,
                                                        mail_attach_filepath, sender, hosp,
                                                        folder)
                            # with mysql.connector.connect(**conn_data) as con:
                            #     cur = con.cursor()
                            #     q = f"insert into utr_mails_temp (`id`,`subject`,`date`,`sys_time`,`attach_path`, `sender`, `hospital`, `utr`, `folder`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            #     data = (
                            #         id, subject, date, str(datetime.now()), mail_attach_filepath,
                            #         sender, hosp, text, folder)
                            #     cur.execute(q, data)
                            #     con.commit()
                        except:
                            log_exceptions(id=id, hosp=hosp, folder=folder)
                            failed_mails(id, date, subject, hosp, folder)
                        signal.alarm(0)
                request = results.list_next(request, msg_col)
    except:
        log_exceptions(hosp=hosp)

def graph_api(data, hosp, deferred, utr, utr2):
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
                with open('logs/folders.log', 'a') as tfp:
                    print(str(datetime.now()), hosp, folder, sep=',', file=tfp)
                flag = 0
                while 1:
                    if flag == 0:
                        query = f'https://graph.microsoft.com/v1.0/users/{email}/messages?$search="{utr2}"'
                    flag = 1
                    graph_data2 = requests.get(query,
                                               headers={'Authorization': 'Bearer ' + result['access_token']}, ).json()
                    if 'value' in graph_data2:
                        for i in graph_data2['value']:
                            signal.signal(signal.SIGALRM, alarm_handler)
                            signal.alarm(time_out)
                            try:
                                date, subject, attach_path, sender = '', '', '', ''
                                format = "%Y-%m-%dT%H:%M:%SZ"
                                b = datetime.strptime(i['receivedDateTime'], format).replace(tzinfo=pytz.utc).astimezone(
                                    pytz.timezone('Asia/Kolkata')).replace(
                                    tzinfo=None)
                                b = b.strftime('%d/%m/%Y %H:%M:%S')
                                date, subject, sender = b, i['subject'], i['sender']['emailAddress']['address']
                                mail_attach_filepath, sett_sno = '', ''
                                temp = get_from_settlement(i['id'], subject, date)
                                if temp is not None:
                                    sett_sno, mail_attach_filepath = temp
                                if mail_attach_filepath == '':
                                    try:
                                        flag = 0
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
                                insert_utr_mails_sett_mails(utr, utr2, sett_sno, i['id'], subject, date,
                                                            mail_attach_filepath, sender, hosp,
                                                            folder)
                                # with mysql.connector.connect(**conn_data) as con:
                                #     cur = con.cursor()
                                #     q = f"insert into utr_mails_temp (`id`,`subject`,`date`,`sys_time`,`attach_path`, `sender`, `hospital`, `utr`, `folder`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                #     data = (
                                #     i['id'], subject, date, str(datetime.now()), mail_attach_filepath,
                                #     sender, hosp, text, folder)
                                #     cur.execute(q, data)
                                #     con.commit()
                            except:
                                log_exceptions(mid=i['id'], hosp=hosp, folder=folder)
                                failed_mails(i['id'], date, subject, hosp, folder)
                            signal.alarm(0)
                    else:
                        with open('logs/query.log', 'a') as fp:
                            print(query, file=fp)
                    if '@odata.nextLink' in graph_data2:
                        query = graph_data2['@odata.nextLink']
                    else:
                        break
    except:
        log_exceptions(hosp=hosp)

def imap_(data, hosp, deferred, utr, utr2):
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
            _, message_numbers_raw = imap_server.search(None, f'(TEXT "{utr2}")')
            for message_number in message_numbers_raw[0].split():
                signal.signal(signal.SIGALRM, alarm_handler)
                signal.alarm(time_out)
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
                    mail_attach_filepath, sett_sno = '', ''
                    temp = get_from_settlement(mid, subject, date)
                    if temp is not None:
                        sett_sno, mail_attach_filepath = temp
                    if mail_attach_filepath == '':
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
                    insert_utr_mails_sett_mails(utr, utr2, sett_sno, mid, subject, date,
                                                mail_attach_filepath, sender, hosp,
                                                folder)
                    # with mysql.connector.connect(**conn_data) as con:
                    #     cur = con.cursor()
                    #     q = f"insert into utr_mails_temp (`id`,`subject`,`date`,`sys_time`,`attach_path`, `sender`, `hospital`, `utr`, `folder`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    #     data = (
                    #         mid, subject, date, str(datetime.now()), mail_attach_filepath,
                    #         sender, hosp, text, folder)
                    #     cur.execute(q, data)
                    #     con.commit()
                except:
                    log_exceptions(subject=subject, date=date, hosp=hosp, folder=folder)
                    failed_mails(mid, date, subject, hosp, folder)
                signal.alarm(0)
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

def search(utr, utr2, hospital, deferred):
    data, hosp = hospital_data[hospital], hospital
    if data['mode'] == 'gmail_api':
        gmail_api(data, hosp, deferred, utr, utr)
        gmail_api(data, hosp, deferred, utr, utr2)
    elif data['mode'] == 'graph_api':
        graph_api(data, hosp, deferred, utr, utr)
        graph_api(data, hosp, deferred, utr, utr2)
    elif data['mode'] == 'imap_':
        imap_(data, hosp, deferred, utr, utr)
        imap_(data, hosp, deferred, utr, utr2)

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

def process_utr_mails(utr):
    utr_orig = utr
    utr_temp = []
    regex = re.compile(r'^[A-Za-z]+')
    temp = regex.search(utr)
    if temp is not None:
        utr = regex.sub('', utr)
    utr_temp_fields = ("sno","hospital","utr","id","subject","date","sys_time","attach_path","sender","folder","completed")
    q = "select * from utr_mails_temp where utr like %s and completed='0'"
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q, ('%' + utr + '%',))
        result = cur.fetchall()
        for row in result:
            temp = {}
            for k, v in zip(utr_temp_fields, row):
                temp[k] = v
            utr_temp.append(temp)
        for i in utr_temp:
            data = (i['id'], i['subject'], i['date'])
            q = "select sno from settlement_mails where id=%s and subject=%s and date=%s limit 1"
            cur.execute(q, data)
            result = cur.fetchone()
            if result is not None:
                q = "insert into utr_mails (`hospital`,`utr`,`utr2`,`completed`,`sett_table_sno`,`id`,`subject`,`date`,`sys_time`,`attach_path`,`sender`,`folder`) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                data = (i['hospital'], utr_orig, i['utr'], '', result[0], i['id'], i['subject'], i['date'], str(datetime.now()), i['attach_path'], i['sender'], i['folder'])
                cur.execute(q, data)
                con.commit()
            else:
                q = "insert into settlement_mails (`id`,`subject`,`date`,`sys_time`,`attach_path`,`completed`,`sender`,`hospital`,`folder`,`process`) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                data = (i['id'], i['subject'], i['date'], str(datetime.now()), i['attach_path'], '', i['sender'], i['hospital'], i['folder'], 'utr_mails')
                cur.execute(q, data)
                con.commit()
                data = (i['id'], i['subject'], i['date'])
                q = "select sno from settlement_mails where id=%s and subject=%s and date=%s limit 1"
                cur.execute(q, data)
                r1 = cur.fetchone()
                if r1 is not None:
                    q = "insert into utr_mails (`hospital`,`utr`,`utr2`,`completed`,`sett_table_sno`,`id`,`subject`,`date`,`sys_time`,`attach_path`,`sender`,`folder`) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    data = (
                    i['hospital'], utr_orig, i['utr'], '', r1[0], i['id'], i['subject'], i['date'], str(datetime.now()),
                    i['attach_path'], i['sender'], i['folder'])
                    cur.execute(q, data)
                    con.commit()
        q = "update utr_mails_temp set completed='X' where utr like %s and completed='0'"
        cur.execute(q, ('%' + utr + '%',))
        q = "update settlement_utrs set search_completed='p' where utr=%s"
        cur.execute(q, (utr,))
        con.commit()

def main():
    hos_settlement_group, utrs, deferred = {}, {}, ''
    q = "select hosp_name, hosp_group from hos_settlement_group where active=1"
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        cur.execute(q)
        result = cur.fetchall()
        for hosp_name, hosp_group in result:
            hos_settlement_group[hosp_group] = []
            utrs[hosp_group] = []
        for hosp_name, hosp_group in result:
            hos_settlement_group[hosp_group].append(hosp_name)
        q = "select utr, hosp_group from settlement_utrs where search_completed=''"
        cur.execute(q)
        result = cur.fetchall()
        for utr, hosp_group in result:
            utrs[hosp_group].append(utr)
    for group in utrs:
        if group in hos_settlement_group and group in utrs:
            hosp_list = hos_settlement_group[group]
            utr_list = utrs[group]
            for utr in utr_list:
                try:
                    ####for test purpose
                    # utr = 'SIN02418Q1047845'
                    # hosp_list = ['ils', 'ils_dumdum', 'ils_howrah', 'ils_agartala', 'ils_ho']
                    ####
                    print(utr)
                    flag = 'p'
                    utr2 = utr
                    regex = re.compile(r'^[A-Za-z]+')
                    temp = regex.search(utr)
                    if temp is not None:
                        utr2 = regex.sub('', utr)
                    for hosp in hosp_list:
                        search(utr, utr2, hosp, deferred)
                    with mysql.connector.connect(**conn_data) as con:
                        cur = con.cursor()
                        q = 'select * from utr_mails where utr=%s limit 1'
                        cur.execute(q, (utr,))
                        result = cur.fetchone()
                        if result is None:
                            flag = 'notfound'
                    with mysql.connector.connect(**conn_data) as con:
                        cur = con.cursor()
                        q = "update settlement_utrs set search_completed=%s where utr=%s"
                        cur.execute(q, (flag, utr,))
                        con.commit()
                except:
                    log_exceptions(utr=utr)

if __name__ == '__main__':
    main()
    pass