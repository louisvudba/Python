import os
import time
from datetime import datetime

def int_to_date(dateInt):
    return datetime.strptime(str(dateInt),"%Y%m%d")
def datetime_to_ms_epoch(dt):
    microseconds = time.mktime(dt.timetuple()) * 1000000 + dt.microsecond
    return int(round(microseconds / float(1000)))

def generate_dump_command(host, port, user, pw, db, collection, query):
    return f"mongodump --host {host} --port {port} --db {db} --collection {collection}  --authenticationDatabase admin --username {user} --password {pw}" + \
    f" --query='{query}'" + \
    f" --archive=/home/sysadmin/{db}.archive --gzip"

fromDate = 20220101
query = '{CreatedDate: { $gte: new Date(' + str(datetime_to_ms_epoch(int_to_date(fromDate))) +  ') }}'

dbList = [1,2,3,4,5,6,8,10,11,13,17,18,19,21,26,30,32,35]
dbPattern = 'kv_noti_'
for db in dbList:
    dbName = f"{dbPattern}{db}"    
    cmd = generate_dump_command("localhost",27017, "", "", dbName, "", f'')
    print(dbName)
    os.system(cmd)

"""
import os

def generateRestoreCmd(dbNo):
    return f"mongorestore --host localhost --port 27017 --db kv_noti_{dbNo} --authenticationDatabase admin --username admin --password 7T9g7s6d" + \
    f" --noIndexRestore --gzip --quiet --archive=/home/lam.vt\@citigo.io/noti_{dbNo}.archive"

dbList = [1,2,3,4,5,6,8,10,11,13,17,18,19,21,26,30,32,35]
for db in dbList:
    cmd = generateRestoreCmd(db)
    print(f"kv_noti_{db}")
    os.system(cmd)
"""

import os

def run_command():
    dbList = [1,2,3,4,5,6,8,9,10,11,12,13,17,18,19,20,21,23,24,26,28,29,30,32,35,36,37]
    #pattern = '/opt/connector/audit_oplog_'
    pattern = "audit_connector_"
    cmdTemplate = "systemctl start "
    for db in dbList:
        cmd = f"{cmdTemplate}{pattern}{db}"
        print(cmd)
        os.system(cmd)

def read_file():
    dbList = [1,2,3,4,5,6,8,9,10,11,12,13,17,18,19,20,21,23,24,26,28,29,30,32,35,36,37]
    pattern = '/opt/connector/audit_'
    for db in dbList:
        dbName = f"kv_audit_{db}"
        with open(f"{pattern}{db}.log", 'r') as fin:
            last_line = fin.readlines()[-1]
        print(f"{dbName}: {last_line}")

read_file()

