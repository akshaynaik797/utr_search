import csv
import re
import mysql.connector

from settings import conn_data

with open('c3.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        for row in csv_reader:
            q = "insert into settlement_utrs (utr, hosp_group, search_completed) values (%s, %s, %s)"
            cur.execute(q, (row[0], '1', ''))
        con.commit()
