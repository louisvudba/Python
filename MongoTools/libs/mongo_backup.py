# def cleanup_by_retailerid_createddate(self, db_name, collection_list, date_str):
#         try:            
#             end_date = Common.convert_str_to_date(date_str) 
#             returnResult = []
#             cleanup_start_time = datetime.now()     
#             client = pymongo.MongoClient(self.connection_string)
#             db = client[db_name]
#             cols = db.list_collection_names(include_system_collections=False)
#             for c in cols:
#                 if (c in collection_list.split(",")):
#                     start_time = datetime.now()

#                     ds = db[c].find_one({}, {"RetailerId": 1}, sort = [("RetailerId", pymongo.ASCENDING)])
#                     min_id = ds["RetailerId"]
#                     ds = db[c].find_one({}, {"RetailerId": 1}, sort = [("RetailerId", pymongo.DESCENDING)])
#                     max_id = ds["RetailerId"]
#                     print(f"{db_name}: {min_id} - {max_id}")

#                     total_deleted = 0                    
#                     isStop = 0
#                     while isStop == 0:                        
#                         ds = db[c].distinct( "RetailerId", { "RetailerId": { "$gte": min_id, "$lt": min_id + self.batch_size }})
#                         rs = db[c].delete_many({ "RetailerId": { "$in": ds }, "CreatedDate": { "$lt": end_date } })
#                         total_deleted += rs.deleted_count
#                         min_id += self.batch_size
#                         if (min_id > max_id):
#                             isStop = 1               
#                     end_time = datetime.now()

#                     # logger.info({"message": f"Cleanup documents in collection {c} - database {db_name}",
#                     #     "total": total_deleted, "validate": total_deleted})
#                     returnResult.append({ "database": db_name, "check_date": date_str, "duration_time": (end_time - start_time).total_seconds(),
#                         "collection": c, "execution_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
#                         "total": total_deleted, "deleted": total_deleted, "exception": "" })

#         except Exception as ex:
#             # log exception
#             print(ex)
#             # logger.exception(ex)
#             cleanup_end_time = datetime.now()
#             returnResult.append({ "database": db_name, "check_date": date_str, "duration_time": (cleanup_end_time - cleanup_start_time).total_seconds(),
#                     "execution_time": cleanup_start_time.strftime("%Y-%m-%d %H:%M:%S"), "total": -1, "deleted": -1, "exception": str(ex) })
#         finally:
#             client.close()
#         return returnResult
    
#     def cleanup_by_createddate(self, db_name, collection_list, date_str):
#         try:          
#             end_date = Common.convert_str_to_date(date_str)  
#             returnResult = []
#             cleanup_start_time = datetime.now()     
#             client = pymongo.MongoClient(self.connection_string)
#             db = client[db_name]
#             cols = db.list_collection_names(include_system_collections=False)
#             for c in cols:
#                 if (c in collection_list.split(",")):
#                     start_time = datetime.now()
#                     rs = db[c].delete_many({ "CreatedDate": { "$lt": end_date } })
#                     total_deleted = rs.deleted_count
#                     end_time = datetime.now()

#                     # logger.info({"message": f"Cleanup documents in collection {c} - database {db_name}",
#                     #     "total": total_deleted, "validate": total_deleted})
#                     returnResult.append({ "database": db_name, "check_date": date_str, "duration_time": (end_time - start_time).total_seconds(),
#                         "collection": c, "execution_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
#                         "total": total_deleted, "deleted": total_deleted, "exception": "" })

#         except Exception as ex:
#             # log exception
#             print(ex)
#             # logger.exception(ex)
#             cleanup_end_time = datetime.now()
#             returnResult.append({ "database": db_name, "check_date": date_str, "duration_time": (cleanup_end_time - cleanup_start_time).total_seconds(),
#                     "execution_time": cleanup_start_time.strftime("%Y-%m-%d %H:%M:%S"), "total": -1, "deleted": -1, "exception": str(ex) })
#         finally:
#             client.close()
#         return returnResult

#     def mongo_backup(self, dbName, args):
#         try:
#             returnResult = []
#             cleanupStartTime = datetime.now()

#             dbConn = self.__open_client_connection(args["connection_type"])
#             secondaryNode = self.__get_replica_node(dbConn, "SECONDARY")
            
#             backupDir = os.path.join(args["backup_dir"], args["dateint"])                         

#             db = dbConn[dbName]            
#             collections = db.list_collection_names(include_system_collections=False)
#             for collection in collections:
#                 startTime = datetime.now()
#                 backupFileName = dbName + "." + collection + ".archive"
#                 command = self.__generate_mongo_dump_archive(secondaryNode['name'].split(":")[0], secondaryNode['name'].split(":")[1], dbName, 
#                     collection, backupDir, backupFileName, Common.generateQuery(Common.convertIntToDate(args["dateint"]), args["daterange"]))
#                 # print(command)
#                 os.system(command)
#                 endTime = datetime.now()
#                 returnResult.append({ "database": dbName, "duration_time": (endTime - startTime).total_seconds(), "collection": collection,
#                         "execution_time": startTime.strftime("%Y-%m-%d %H:%M:%S"), "exception": "" })                
#         except Exception as ex:
#             # log exception
#             print (ex)
#             logger.exception(ex)
#             cleanupEndTime = datetime.now()
#             returnResult.append({ "database": dbName, "duration_time": (cleanupEndTime - cleanupStartTime).total_seconds(), 
#                     "execution_time": cleanupStartTime.strftime("%Y-%m-%d %H:%M:%S"), "exception": str(ex) })
#         return returnResult

#     def mongo_get_dblist(self, args):
#         try:
#             db = self.__open_client_connection(args["connection_type"])
#             return db.list_database_names()
#         except Exception as ex:
#             # log exception
#             print (ex)
#             logger.exception(ex)

#     def do_mongo(self, db, args, func):
#         default = "Oops! Wrong Input"        
#         return getattr(self, 'cleanup_by_' + func, lambda: default)(db, args)