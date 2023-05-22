import multiprocessing as mp
from libs.logger import Logger
from libs.mongo import MongoImpl
from datetime import datetime, timedelta

config = {
    "host": "*",
    "username": "admin",
    "password": "*",
    "authen_source": "admin",
    "replica_set": "*"
}

date_range = 10
date_str = (datetime.now() - timedelta(days=date_range)).strftime("%Y%m%d")
log_file = "log/noti_cleanup.log"
collection = "Notification"

db_pattern = "kv_noti_"
db_list = ["1","2","4","5","17","28","32","36","3","6","8","9","10","11","12","13","14","18","19","20","21","23","24","26","29","30","35","37"]
#db_list = ["1"]

def mongo_cleanup(database_name, collection, date_str):
    mongo = MongoImpl(**config)
    mongo.database_name = database_name
    mongo.collection = collection
    return mongo.cleanup_by_objectid(date_str)

if __name__ == '__main__':
    pool = mp.Pool(mp.cpu_count() - 1)        
    for db_no in db_list:
        lamda_function = lambda result: Logger.push_logger(result, log_file)
        pool.apply_async(mongo_cleanup, args = (f"{db_pattern}{db_no}", collection, date_str), callback=lamda_function)
    pool.close()
    pool.join()