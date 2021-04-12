#!/usr/bin/python3
import os
import pymongo
import ftplib
from datetime import datetime, timedelta
from os import path
from ftplib import FTP, error_perm

'''
    mongo backup by python
'''

# configs:
outputs_dir = '/opt/backup/mongodb/'
outputs_ext = '.gz'
host = "localhost" # if host is your local machine leave it NA
port = 27017 # if mongo is on default port (37017) leave in NA

authenticationDatabase = "admin"
username = "admin" # if there is no username set, leave it in NA
password = "7T9g7s6d" # if there is no password set, leave it in NA

db_filter = ["kvfb"]

def main():
    backup_date = (datetime.now() -  timedelta(days=1)).strftime("%Y%m%d")

    print(datetime.now().strftime("%H:%M:%S") + " - Mongo backup progress started")
    run_backup(username, password, host, port, backup_date)
    print(datetime.now().strftime("%H:%M:%S") + " - Mongo backup done")    

def run_backup(username, password, host, port, backup_date):
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d000000")
    end_date = datetime.now().strftime("%Y%m%d000000")
    db_con = "mongodb://" + username + ":" + password + "@" + host + ":" + str(port)
    db_client = pymongo.MongoClient(db_con)
    
    if (db_client['admin'].command('replSetGetStatus')['myState'] != 1):
        print("SECONDARY")
        return

    # check db in mongo matching db_filter
    for db_item in db_client.list_databases():
        is_backup = False
        #print("db - " + db_name['name'])
        for i in db_filter:
            #print("filter " + i)
            if i in db_item['name']:
                is_backup = True
                break
        # backup diff all collections
        if is_backup:
            db = db_client[db_item['name']]
            db_collection = db.collection_names(include_system_collections=False)
            prepare_dir(db_item['name'], backup_date)
            for collection in db_collection:
                total_docs = db.collection[collection].find({"time": {"$gt": start_date, "$lt": end_date }}).count()
                if (total_docs > 0):
                    command = "mongodump --host " + host + " --port " + str(port) + " -d " + db_item['name'] + " --authenticationDatabase " + authenticationDatabase
                    command += " --username " +  username + " --password " + password
                    command += " -c " + collection
                    command += " --archive=" + render_output_locations(db_item['name'], backup_date, collection) + " --gzip"
                    command += " --query " + render_filter(start_date, end_date)
                    print(">> " + collection + " - Total: " + str(total_docs))                
                    os.system(command)
            source_path = outputs_dir + db_item['name'] + "/" + backup_date + "/"
            do_ftp_upload(db_item['name'], backup_date, source_path)

def render_output_locations(db_name, backup_date, collection_name):
    return outputs_dir + db_name + "/" + backup_date + "/" + db_name + "_" + collection_name + "_" + backup_date + outputs_ext

def render_filter(start_date, end_date):
    return "'{time :{ $gt: " + start_date + ", $lt: " + end_date + " }}'"        


def prepare_dir(db_name, backup_date):
    try:
        path = os.path.join(outputs_dir, db_name)
        os.makedirs(path, exist_ok = True)
        print("Directory '%s' created successfully" % db_name)
    except OSError as error:
        print("Directory '%s' can not be created" % db_name)

    try:
        path = os.path.join(outputs_dir, db_name, backup_date)
        os.makedirs(path, exist_ok = True)
        print("Directory '%s' created successfully" % backup_date)
    except OSError as error:
        print("Directory '%s' can not be created" % backup_date)

def do_ftp_upload(db_name, backup_date, source_path):
    host = '192.168.101.55'
    port = 21

    full_path = "/" + db_name + "/" + backup_date + "/"

    ftp = FTP()
    ftp.connect(host,port)
    ftp.login('kvmongoR','kvmongoRR')
    try:
        ftp.mkd(db_name)
        ftp.cwd(db_name)
    except error_perm:
        ftp.cwd(db_name)
    try:    
        ftp.mkd(backup_date)
        ftp.cwd(backup_date)
    except error_perm:
        ftp.cwd(backup_date)

    for name in os.listdir(source_path):
        localpath = os.path.join(source_path, name)
        if os.path.isfile(localpath):
            print("STOR", name, localpath)
            ftp.storbinary('STOR ' + name, open(localpath,'rb'))
    ftp.quit()

if __name__ == "__main__":
    main()