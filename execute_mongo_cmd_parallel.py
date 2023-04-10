import subprocess
import multiprocessing as mp
import time
from datetime import date, datetime, timedelta

def date_to_datetime(mDate,mTime):
    return datetime.combine(mDate, mTime)

def datetime_to_ms_epoch(dt):
    microseconds = time.mktime(dt.timetuple()) * 1000000 + dt.microsecond
    return int(round(microseconds / float(1000)))

def generate_shell_cmd(host,user,pwd,authSource,replSet,query):
    if (replSet == ""):
        cmd = f"mongo --quiet --host \"mongodb://{host}/?authSource={authSource}\" -u {user} -p {pwd} --eval '{query}' | grep -v 'I NETWORK'"
    else:
        cmd = f"mongo --quiet --host \"mongodb://{host}/?authSource={authSource}&replicaSet={replSet}\" -u {user} -p {pwd} --eval '{query}' | grep -v 'I NETWORK'"
    return cmd

def execute_command(db,collection,checkDate):
    user = "admin"
    pwd = "7T9g7s6d"
    host = "dc2p-retail-mongo-tracking-01.citigo.io:27017,dc2p-retail-mongo-tracking-02.citigo.io:27017,dc2p-retail-mongo-tracking-03.citigo.io:27017"
    authSource = "admin"
    replSet = "dc2p-retail-tracking"
    logFile = "/opt/cleanup_tracking.log"

    checkDateTS = datetime_to_ms_epoch(date_to_datetime(checkDate, datetime.min.time()))
    condition = "{CreatedAt: {$lt: new Date(" + str(checkDateTS) +")}, Status:2}"

    #query = f"db.getSiblingDB(\"{db}\").getCollection(\"{collection}\").deleteMany({condition})"
    query = f"db.getSiblingDB(\"{db}\").getCollection(\"{collection}\").countDocuments({condition})"
    cmd = generate_shell_cmd(host,user,pwd,authSource,replSet,query)
    #print(cmd)
    result = subprocess.check_output(cmd, shell=True)

    file_object = open(logFile, 'a')
    file_object.write(f"{checkDate} - {db} - {collection} - {condition} - Total Docs: " + result.decode())
    file_object.close()

db = "kiotviet"
checkDate = date.today() - timedelta(days = 1)

collections = ["BalanceTrackingObject","BatchExpireTrackingObject","ComboTrackingObject","ImeiTrackingObject"
                ,"InventoryTrackingObject","OrderTrackingObject","PointTrackingObject","StockTrackingObject"
                ,"TrackingTransactionBeforeObject","TrackingTransactionObject","TransferSyncEsToKafkaEventObject"]

for col in collections:
    execute_command(db,col,checkDate)
