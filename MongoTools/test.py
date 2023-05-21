from libs.mongo import MongoImpl
from libs.common import Common

config = {
    "host": "",
    "username": "",
    "password": "",
    "authen_source": "",
    "replica_set": ""
}
mongo = MongoImpl(**config)
#mongo.cleanup_by_object("20230220")
print(mongo._get_objectid(Common.convert_str_to_date_utc('20230205')))

