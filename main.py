from apscheduler.schedulers.background import BackgroundScheduler
from settings import hospital_data, pdfconfig, file_no, file_blacklist, conn_data, interval
from mail_storage import gmail_api, graph_api, imap_

sched = BackgroundScheduler(daemon=False)
for hosp, data in hospital_data.items():
    if data['mode'] == 'gmail_api':
        # print(hosp)
        # gmail_api(data, hosp)
        sched.add_job(gmail_api, 'interval', seconds=interval, args=[data, hosp], max_instances=1)
    elif data['mode'] == 'graph_api':
        # print(hosp)
        # graph_api(data, hosp)
        sched.add_job(graph_api, 'interval', seconds=interval, args=[data, hosp], max_instances=1)
    elif data['mode'] == 'imap_':
        # imap_(data, hosp)
        sched.add_job(imap_, 'interval', seconds=interval, args=[data, hosp], max_instances=1)
sched.start()
print('Scheduler running')