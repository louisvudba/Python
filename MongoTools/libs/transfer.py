import os
import ftplib
import pysftp
import shutil
from ftplib import FTP, error_perm
from pathlib import Path
#from libs.setup_logger import logger

class Transfer:
    def __init__(self, args):
        self.host = args["host"]
        self.port = args["port"]
        self.username = args["username"]
        self.password = args["password"]
        
    def ftp_transfer(self, dbName, dateInt, sourceDir):
        try:
            ftp = FTP()
            ftp.connect(self.host, self.port)
            ftp.login(self.username, self.password)
            try:
                ftp.mkd(dbName)
                ftp.cwd(dbName)
            except error_perm:
                ftp.cwd(dbName)
            try:    
                ftp.mkd(dateInt)
                ftp.cwd(dateInt)
            except error_perm:
                ftp.cwd(dateInt)

            for item in os.listdir(sourceDir):
                localFile = os.path.join(sourceDir, item)
                if os.path.isfile(localFile):
                    print("STOR", localFile, localFile)
                    is_uploaded = False
                    while not is_uploaded:
                        try:                    
                            ftp.storbinary('STOR ' + localFile, open(localFile,'rb'))
                            is_uploaded = True
                        except ftplib.all_errors:
                            print ("retry uploading...")
                            is_uploaded = False
                        else:
                            is_uploaded = True
                            print (item, " uploaded")
            ftp.quit()
            shutil.rmtree(sourceDir)        
        except Exception as ex:
            # log exception
            print(ex)
            logger.exception(ex)

    def sftp_transfer(self, args):
        cnopts1 = pysftp.CnOpts(knownhosts=args["known_host_path"])
        try:
            with pysftp.Connection(self.host, username=self.username, password=self.password, cnopts=cnopts1) as sftp:
                # check backup date folder exist
                try:
                    sftp.chdir(args["dest_dir"])
                except IOError:
                    sftp.mkdir(args["dest_dir"])
                    sftp.chdir(args["dest_dir"])

                try:
                    sftp.chdir(args["folder_name"])
                except IOError:
                    sftp.mkdir(args["folder_name"])
                    sftp.chdir(args["folder_name"])

                for item in os.listdir(args["source_dir"]):
                    filePath = os.path.join(args["source_dir"], item)
                    isTransferred = False
                    while (isTransferred == False):
                        res = sftp.put(filePath)  # upload file to public/ on remote
                        if (res.st_size == Path(filePath).stat().st_size):
                            isTransferred = True
            # remove backup file
            shutil.rmtree(args["source_dir"])
        except Exception as ex:
            # log exception
            print(ex)
            #logger.exception(ex)
