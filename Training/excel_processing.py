import multiprocessing as mp
import string
import sys, getopt, os, shutil
import pandas as pd
import json, numbers, re
from datetime import datetime
from win32com import client
import win32api

config = 'config.json'
with open(config, encoding="utf8") as json_file: # encoding for valid character
    data = json.load(json_file)

root_folder = data['root_folder']
input_folder = root_folder + '\Input'
output_folder = root_folder + '\Output'
archive_folder = root_folder + '\Archive'

# get file list
file_list = [x for x in os.listdir(input_folder) if x.endswith(data['extension'])]
filter_dict = data["filter_chars"]
filter_dict1 = data["filter_charsbill"]
filter_dict2 = data["filter_productname"]

def main(args):
    short_options = "ht:"
    long_options = ["help", "type="]

    try:
        arguments, values = getopt.getopt(args, short_options, long_options)
    except getopt.error as err:
        # Output error, and return with an error code
        print (str(err))
        sys.exit(2)
   
    for arg, value in arguments:
        if arg in ("-h", "--help"):
            print ("Displaying help")
        elif arg in ("-t", "--type"):
            itype = value.lower()    
    
    Choice(itype)

def Choice(itype):    
    switcher = {
            "clean": clean_data,
            "cleanbill": clean_bill,
            "cleanshipper": clean_shippername,
            "data": filter_data,
            "loctenhang": filter_data1,
            "unit": filter_unit,
            "tenhang": crossjoin_data,
            "tuongdoi": thaythe_tuongdoi,
            "chinhxac": thaythe_chinhxac,
            "pdf": excel2pdf,
            "debug": ExcelMultiProcessing.process_multithreading
        }
    return switcher.get(itype, lambda: "Invalid!!")()

class ExcelMultiProcessing:
    @staticmethod
    def process_multithreading():       
        sheet_id = '127wQImmXU_M1YQeXYdSKvv6WO9FYzJP9Xg0ukoBrNKY'
        pool = mp.Pool(mp.cpu_count() - 1)  # số threading = tổng thread - 1 ~ để 1 thread cho OS
        splitSize = 50000 # Split total records base on size
        start_time = datetime.now()
        
        df_filter = pd.read_excel(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx")
        filter_dict = df_filter.to_dict(orient='records')     
        fileList = []
        
        for file in file_list:
            fileList.append(file)
            input_file = os.path.join(input_folder, file)
            
            print('Processing ' + file + ' ...')            
            df = pd.read_excel(input_file)           
            totalRow = len(df)
            startIndex = 0
            step = 1
            while (startIndex < totalRow):
                endIndex = startIndex + splitSize
                if (endIndex > totalRow):
                    endIndex = totalRow
                output_file = os.path.join(output_folder, 'output_text_' + file.replace('.xlsx','_' + str(step) + '.xlsx'))
                pool.apply_async(ExcelMultiProcessing.process_thread, args = (df.iloc[startIndex:endIndex], filter_dict, output_file))
                startIndex += splitSize
                step += 1
               
        pool.close()
        pool.join()           
        
        print("Done")
        print("Merging files ...")
        ExcelMultiProcessing.merge_file(fileList)
        print("Done")
        time_elapsed = datetime.now() - start_time
        print('Tong thoi gian xu ly {}'.format(time_elapsed)) 

    @staticmethod
    def merge_file(fileList):
        fileCount = 1
        for file in fileList:  
            input_file = os.path.join(input_folder, file)          
            output_file = os.path.join(output_folder, 'output_text_' + file)
            archive_file = os.path.join(archive_folder, file)

            print('File ' + str(fileCount))
            shutil.move(input_file, archive_file)
            childFileList = [x for x in sorted(os.listdir(output_folder)) if x.endswith(data['extension']) and file.replace('.xlsx','') in x]

            df = pd.DataFrame()
            di = pd.DataFrame()
            i = 1
            
            for item in childFileList:                
                if (i == 1):
                    di = pd.read_excel(os.path.join(output_folder, item))
                else:
                    di = pd.read_excel(os.path.join(output_folder, item), index_col=None, header=None)               
                df = df.append(di, ignore_index=True)
                os.remove(os.path.join(output_folder, item))

            fileCount += 1
            df.to_excel(output_file, index = False)
            print(output_file)

    @staticmethod
    def process_thread(df,filter_dict,output_file):
        print(output_file)
        r, c = df.shape
        i = 0
        j = 0

        while i < r:
            j = 0
            while j < c:
                cell_value = df.iat[i,j]          
                if (pd.isna(cell_value)):
                    text_value = ''
                else:
                    text_value = str(cell_value)
                text_len = len(text_value)
        
                result = ''
                for item in filter_dict:
                    filtername1 = str(item['Name']).lower()
                    pos1 = text_value.lower().find(filtername1)
                    if pos1 >= 0:
                        lenfilter = len(filtername1)
                        result = text_value[0:pos1] + text_value [pos1+lenfilter:]
                    else:
                         result = text_value        
                    text_value = result 
                df.iat[i,j] = result
                j += 1 
                #print(i)
            i += 1
        
        df.to_excel(output_file, index = False)


def crossjoin_data():
    #crossjoin_col = data["crossjoin_column"]
    start_time = datetime.now()
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_PDF_' + file)
        archive_file = os.path.join(archive_folder, file)
        print('processing ' + file)
        print(datetime.now())    
        
        df = pd.read_excel(input_file, keep_default_na=False)
  
        list1 = [f'{a}, {b}, {c}, {d}, {e}' 
          for a in df.item if a != '' 
          for b in df.ex1 if b != '' 
          for c in df.ex2 if c != ''
          for d in df.ex3 if d != ''
          for e in df.ex4 if e != ''
    
            ]
        dfRes = pd.DataFrame(data=list1)
        #print(df)
        dfRes.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
    time_elapsed = datetime.now() - start_time    
    print("xử lý dữ liệu thành công")
    print('Tong thoi gian xu ly {}'.format(time_elapsed))

def excel2pdf():
    #crossjoin_col = data["crossjoin_column"]
    start_time = datetime.now()
    
    for file in file_list:
        excel = client.Dispatch("Excel.Application")
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_crossjoin_' + file)
        archive_file = os.path.join(archive_folder, file)
        sheets = excel.Workbooks.Open(input_file)
        work_sheets = sheets.Worksheets[0]
        print('processing ' + file)
        print(datetime.now())    
        work_sheets.ExportAsFixedFormat(0,output_file)
        sheets.Close()
        excel.Quit()
        shutil.move(input_file, archive_file)
    time_elapsed = datetime.now() - start_time    
    print("xử lý excel sang pdf thành công")
    print('Tong thoi gian xu ly {}'.format(time_elapsed))

def clean_data():
    start_time = datetime.now()
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_clean_' + file)
        archive_file = os.path.join(archive_folder, file)
        print('processing ' + file)
        print(datetime.now())    
        
        df = pd.read_excel(input_file, keep_default_na=False)

        r, c = df.shape
        i = 0
        j = 0

        while i < r:            
            j = 0
            while j < c:
                cell_value = df.iat[i,j]
                if i == 0:                    
                    print (cell_value)
                if not (isinstance(cell_value, numbers.Real)):
                    if (pd.isna(cell_value)):
                        text_value = ''
                    else:
                        text_value = str(cell_value)
                    text_len = len(text_value)
                    k = 0        
                    result = ''
                    while k < text_len :
                        if filter_dict.find(text_value[k]) > 0 :
                            result += text_value[k]
                        else:
                            result +=' '  
                        k += 1               
                    df.iat[i,j] = result
                else:
                    df.iat[i,j] = cell_value
                j += 1    
                print(i)    
            i += 1

        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)        
        
    print("Done clean_data")
    time_elapsed = datetime.now() - start_time
    print('Tong thoi gian xu ly {}'.format(time_elapsed))  

def clean_shippername():
    start_time = datetime.now()
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_clean_' + file)
        archive_file = os.path.join(archive_folder, file)
        print('processing ' + file)
        print(datetime.now())    
        
        df = pd.read_excel(input_file, keep_default_na=False)

        r, c = df.shape
        i = 0
        j = 0

        while i < r:            
            j = 0
            while j < c:
                cell_value = df.iat[i,j]
                if i == 0:                    
                    print (cell_value)
                if not (isinstance(cell_value, numbers.Real)):
                    if (pd.isna(cell_value)):
                        text_value = ''
                    else:
                        text_value = str(cell_value)
                    text_len = len(text_value)
                    k = 0        
                    result = ''
                    while k < text_len :
                        if filter_dict2.find(text_value[k]) > 0 :
                            result += text_value[k]
                        else:
                            result +=' '  
                        k += 1               
                    df.iat[i,j] = result
                else:
                    df.iat[i,j] = cell_value
                j += 1    
                print(i)    
            i += 1

        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        
    print("Done clean_data")
    time_elapsed = datetime.now() - start_time
    print('Tong thoi gian xu ly {}'.format(time_elapsed))  

def clean_bill():
    start_time = datetime.now()
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_bill_' + file)
        archive_file = os.path.join(archive_folder, file)
        print('processing ' + file)
        print(datetime.now())    
        
        df = pd.read_excel(input_file, keep_default_na=False)

        r, c = df.shape
        i = 0
        j = 0

        while i < r:            
            j = 0
            while j < c:
                cell_value = df.iat[i,j]
                if i == 0:                    
                    print (cell_value)
                if not (isinstance(cell_value, numbers.Real)):
                    if (pd.isna(cell_value)):
                        text_value = ''
                    else:
                        text_value = str(cell_value)
                    text_len = len(text_value)
                    k = 0        
                    result = ''
                    while k < text_len :
                        if filter_dict1.find(text_value[k]) > 0 :
                            result += text_value[k]
                        else:
                            result +=''  
                        k += 1               
                    df.iat[i,j] = result
                else:
                    df.iat[i,j] = cell_value
                j += 1    
                print(i)    
            i += 1

        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        
    print("Done clean_data")
    time_elapsed = datetime.now() - start_time
    print('Tong thoi gian xu ly {}'.format(time_elapsed))  

def filter_data():
    filter_file = os.path.join(root_folder, data["filter_text_file"])
    df_filter = pd.read_excel(filter_file)
    filter_dict = df_filter.to_dict(orient='records')
    start_time = datetime.now()
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_text_' + file)
        archive_file = os.path.join(archive_folder, file)
        print('processing ' + file)
        print(datetime.now())    
        
        df = pd.read_excel(input_file)

        r, c = df.shape
        i = 0
        j = 0

        while i < r:
            j = 0
            while j < c:
                cell_value = df.iat[i,j]          
                if (pd.isna(cell_value)):
                    text_value = ''
                else:
                    text_value = str(cell_value)
                text_len = len(text_value)
        
                result = ''
                for item in filter_dict:
                    if pd.isna(item['Filter']):
                        filter_value = ''
                    else:
                        filter_value = item['Filter']  
                  #  if (text_len == len(item['Name'])):
                    ignore_case = re.compile(re.escape(item['Name']), re.IGNORECASE)
                    result = ignore_case.sub(filter_value, text_value)
                    text_value = result
                df.iat[i,j] = result
                j += 1 
                print(i)
            i += 1

        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        
    print("Done filter_data")
    time_elapsed = datetime.now() - start_time
    print('Tong thoi gian xu ly {}'.format(time_elapsed))  

def filter_data1():
    #filter_file = os.path.join(root_folder, data["filter_text_file"])
    #df_filter = pd.read_excel(filter_file)
    sheet_id = '127wQImmXU_M1YQeXYdSKvv6WO9FYzJP9Xg0ukoBrNKY'
    df_filter = pd.read_excel(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx")
    filter_dict = df_filter.to_dict(orient='records')
    start_time = datetime.now()
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_text_' + file)
        archive_file = os.path.join(archive_folder, file)
        print('processing ' + file)
        print(datetime.now())    
        
        df = pd.read_excel(input_file)

        r, c = df.shape
        i = 0
        j = 0

        while i < r:
            j = 0
            while j < c:
                cell_value = df.iat[i,j]          
                if (pd.isna(cell_value)):
                    text_value = ''
                else:
                    text_value = str(cell_value)
                text_len = len(text_value)
        
                result = ''
                for item in filter_dict:
                    filtername1 = str(item['Name']).lower()
                    pos1 = text_value.lower().find(filtername1)
                    if pos1 >= 0:
                        lenfilter = len(filtername1)
                        result = text_value[0:pos1] + text_value [pos1+lenfilter:]
                    else:
                         result = text_value        
                    text_value = result 
                df.iat[i,j] = result
                j += 1 
                print(i)
            i += 1
        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        
    print("Done filter_data")
    time_elapsed = datetime.now() - start_time
    print('Tong thoi gian xu ly {}'.format(time_elapsed))  

def thaythe_chinhxac():
    #filter_file = os.path.join(root_folder, data["thaythetukhoa"])
    #df_filter = pd.read_excel(filter_file)
    sheet_id = '1FQBGYDZJaIkB6nYw5Veyu7D-nlqGCrscNvW-qmA3YcA'
    df_filter = pd.read_excel(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx")
    filter_dict = df_filter.to_dict(orient='records')
    start_time = datetime.now()
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_text_' + file)
        archive_file = os.path.join(archive_folder, file)
        print('processing ' + file)
        print(datetime.now())    
        
        df = pd.read_excel(input_file)

        r, c = df.shape
        i = 0
        j = 0

        while i < r:
            j = 0
            while j < c:
                cell_value = df.iat[i,j]          
                if (pd.isna(cell_value)):
                    text_value = ''
                else:
                    text_value = str(cell_value)
                text_len = len(text_value)
        
                result = ''
                for item in filter_dict:
                    if pd.isna(item['Filter']):
                        filter_value = ''
                    else:
                        filter_value = item['Filter']  
                    filter_a = str(text_value).lower()
                    filter_b = str(item['Name']).lower()
                    if (filter_a == filter_b):
                        #print(filter_a)
                    #if (text_len == len(item['Name'])): # text_len: length of data, len(item['Name']): length of each filter rule
                        ignore_case = re.compile(re.escape(item['Name']), re.IGNORECASE)
                        result = ignore_case.sub(filter_value, text_value)
                        text_value = result

                df.at[i,j] = text_value
                j += 1 
                print(i)
            i += 1

        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        print(datetime.now())
    time_elapsed = datetime.now() - start_time 
    print('Tong thoi gian xu ly {}'.format(time_elapsed))  
    print("Done filter_data")

def filter_unit():
    filter_file = os.path.join(root_folder, data["filter_unit_file"])
    df_filter = pd.read_excel(filter_file)
    #sheet_id = '1Coa8VmfgRoSF-6gfoAq2aaqUMjJGWAJhfIxF5sm4y6A'
    #df_filter = pd.read_excel(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx")    
    filter_dict = df_filter.to_dict(orient='records')
    start_time = datetime.now()
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_unit_' + file)
        archive_file = os.path.join(archive_folder, file)
        print('processing ' + file)
        print(datetime.now())    
        
        df = pd.read_excel(input_file)

        r, c = df.shape
        i = 0
        j = 0


        while i < r:
            j = 0
            while j < c:
                cell_value = df.iat[i,j]          
                if (pd.isna(cell_value)):
                    text_value = ''
                else:
                    text_value = str(cell_value)        
                result = data["default_unit"]
                
                for item in filter_dict:                
                    if pd.isna(item['Filter']):
                        filter_value = ''
                    else:
                        filter_value = item['Filter']  
                    #text_find = item['Name'].lower()
                    #text_findok = text_find[0:14]  
                    #if text_value.lower().find(text_findok) > -1:                 
                    if text_value.lower().find(item['Name'].lower()) > -1:
                    #if re.search(item['Name'].lower(),text_value.lower()):
                        result = filter_value
                        break
                df.at[i,j] = result                
                j += 1 
                print(i)
            i += 1
        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        
    print("Done filter_unit")
    time_elapsed = datetime.now() - start_time
    print('Tong thoi gian xu ly {}'.format(time_elapsed))

def thaythe_tuongdoi():
    sheet_id = '1pLvt6h-N-vUWNK1daESy6g0Vlcxd-PES6QAslkcB8ZM'
    #sheet_id = '1FQBGYDZJaIkB6nYw5Veyu7D-nlqGCrscNvW-qmA3YcA'
    df_filter = pd.read_excel(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx")
    filter_dict = df_filter.to_dict(orient='records')
    start_time = datetime.now()
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_text_' + file)
        archive_file = os.path.join(archive_folder, file)
        print('processing ' + file)
        print(datetime.now())   
        
        df = pd.read_excel(input_file)

        r, c = df.shape
        i = 0
        j = 0

        while i < r:
            j = 0
            while j < c:
                m=1000
                n=0
                cell_value = df.iat[i,j]          
                if (pd.isna(cell_value)):
                    text_value = ''
                else:
                    text_value = str(''+ cell_value + '').lower()
            
                result = text_value
                #print(result)
                for item in filter_dict:
                    filter_value = str(item['Filter'])
                    filter_name = str(item ['Name'])
                    if filter_name == '':
                        filter_name = text_value
                    k = (' ' + text_value + ' ').find(' ' + filter_name.lower() +' ')
                    #k = (text_value).find(filter_name.lower())
                    if k>=0:
                        if k==m:
                            if len(filter_name)>=n:
                                n=len(filter_name)
                                ignore_case = re.compile(re.escape(text_value),re.IGNORECASE)
                                result = ignore_case.sub(filter_value,text_value)
                        elif k < m:
                            m = k
                            n = 0
                            ignore_case = re.compile(re.escape(text_value),re.IGNORECASE)
                            result = ignore_case.sub(filter_value,text_value)
                df.at[i,j] = result    
                j += 1 
                print(i)
                
            i += 1

        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        print(datetime.now())
    time_elapsed = datetime.now() - start_time 
    print('Tong thoi gian xu ly {}'.format(time_elapsed))  
    print("Done filter_data")

if __name__ == "__main__":
   main(sys.argv[1:])
   #jobs = []
   #p11 = multiprocessing.Process(target=thaythe_tukhoa)
   #jobs.append(p11)
   #p11.start()