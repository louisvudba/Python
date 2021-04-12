from datetime import datetime
from datetime import timedelta

print((datetime.now() -  timedelta(days=1)).strftime("%Y%m%d"))