fields = ["id", "subject", "date", "sys_time", "attach_path", "completed", "sender", "hospital", "insurer", "process",
          "deferred", "sno", "mail_folder"]
data = {'id': 'sad', "subject": 'asddas', "sno": '123'}

q = "update all_mails set "
params = []
for i in data:
    if i in fields and i != 'sno':
        q = q + f"`{i}`=%s, "
        params.append(data[i])
q = q + "where `sno`=%s"
params.append(data['sno'])
q = q.replace(', where', ' where')
pass