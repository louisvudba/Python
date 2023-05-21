import pymongo
import os
import bson
from datetime import datetime
from libs.common import Common

class Mongo:
    def __init__(self, **kwargs):
        self.host = kwargs["host"]
        self.username = kwargs["username"]
        self.password = kwargs["password"]
        self.authen_source = kwargs["authen_source"]
        self.replica_set = kwargs["replica_set"]

    def __repr__(self):
        pass

    def _get_objectid(self, date):
        return bson.ObjectId.from_datetime(date)

    @classmethod
    def change_batch_size(cls, value):
        cls.batch_size = value

    @property
    def connection_string(self):
        if self.replica_set == "":
            return f"mongodb://{self.username}:{self.password}@{self.host}/?authSource={self.authen_source}&replicaSet={self.replica_set}&readPreference=secondary"
        else:
            return f"mongodb://{self.username}:{self.password}@{self.host}/?authSource={self.authen_source}"

    @connection_string.setter
    def connection_string(self, value):
        pass

class MongoImpl(Mongo):
    def __init__(self, **kwargs):
        self._database_name = ""
        self._collection = ""
        self._backup_dir = ""
        self._file_name = ""
        super().__init__(**kwargs)

    def gen_cmd_mongodump(self, query = None):
        backup_path = os.path.join(self.backup_dir, self.file_name)
        uri = self.connection_string.replace("?authSource",f"{self.database_name}?authSource")
        return f"mongodump --uri=\"{uri}\"" + \
            f" --collection={self.collection}" + \
            f" --query='{query}'" if not (query is None) else "" + \
            f" --archive={backup_path}" + \
            " --quiet --gzip" + \
            " | grep -v 'I NETWORK'"

    def gen_cmd_mongorestore(self):
        backup_path = os.path.join(self.backup_dir, self.file_name)
        uri = self.connection_string.replace("?authSource",f"{self.database_name}?authSource")
        return f"mongorestore --uri=\"{uri}\"" + \
            f" --nsInclude={self.database_name}.{self.collection}" + \
            f" --archive={backup_path}" + \
            " --quiet --noIndexRestore --gzip" + \
            " | grep -v 'I NETWORK'"

    def cleanup_by_objectid(self, date_str, batch_size = None):        
        try:
            if batch_size is None: batch_size = 1000
            max_objectid = self._get_objectid(Common.convert_str_to_date_utc(date_str))
            result = []
            cleanup_start_time = datetime.now()
            client = pymongo.MongoClient(super().connection_string)
            db = client[self.database_name]
            cols = db.list_collection_names(include_system_collections=False)
            if (self.collection in cols):                
                start_time = datetime.now()
                total_rows = db[self.collection].count_documents({ "_id": { "$lt": max_objectid } })
                rows = total_rows
                total_deleted = 0
                print("Total rows:", rows)
                while (rows > 0):
                    if rows < batch_size: batch_size = rows
                    ds = db[self.collection].find({ "_id": { "$lt": max_objectid } }, { "_id": 1 }).limit(batch_size)
                    rows = rows - batch_size
                    delete_ids = [item["_id"] for item in ds]
                    rs = db[self.collection].delete_many({ "_id": { "$in": delete_ids } })
                    total_deleted += rs.deleted_count
                    print("Remaining:", rows)
                end_time = datetime.now()

                result.append({ "database": self.database_name, "check_date": date_str, "duration_time": (end_time - start_time).total_seconds(),
                    "collection": self.collection, "execution_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "total": total_rows, "deleted": total_deleted, "exception": "" })

        except Exception as ex:
            # log exception
            print(ex)
            # logger.exception(ex)
            cleanup_end_time = datetime.now()
            result.append({ "database": self.database_name, "check_date": date_str, "duration_time": (cleanup_end_time - cleanup_start_time).total_seconds(),
                    "execution_time": cleanup_start_time.strftime("%Y-%m-%d %H:%M:%S"), "total": -1, "deleted": -1, "exception": str(ex) })
        finally:
            client.close()
        print(result)
        return result

    @property
    def database_name(self):
        return self._database_name

    @database_name.setter
    def database_name(self, value):
        self._database_name = value

    @property
    def collection(self):
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = value

    @property
    def backup_dir(self):
        return self._backup_dir

    @backup_dir.setter
    def backup_dir(self, value):
        self._backup_dir = value

    @property
    def file_name(self):
        return self._file_name

    @file_name.setter
    def file_name(self, value):
        self._file_name = value