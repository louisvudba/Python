import sys, getopt, os, shutil
import pandas as pd
import json, numbers, re
from datetime import datetime

config = 'config.json'
with open(config, encoding="utf8") as json_file: # encoding for valid character
    data = json.load(json_file)

input_folder = data['root_folder'] + '\Input'
output_folder = data['root_folder'] + '\Output'
archive_folder = data['root_folder'] + '\Archive'

# get file list
file_list = [x for x in os.listdir(input_folder) if x.endswith(data['extension'])]
filter_dict = data["filter_chars"]

def main(arg):
    short_options = "ht:"
    long_options = ["help", "type="]

    try:
        arguments, values = getopt.getopt(argument_list, short_options, long_options)
    except getopt.error as err:
        # Output error, and return with an error code
        print (str(err))
        sys.exit(2)
    
    for current_argument, current_value in arguments:
        if current_argument in ("-h", "--help"):
            print ("Displaying help")
        elif current_argument in ("-t", "--type"):
            itype = current_value.lower()
        
    Choice(itype)

def Choice(itype):
    switcher = {
            1: clean_data,
            2: filter_data,
            3: filter_unit
        }
    return switcher.get(argument, lambda: "Invalid!!")() 

def clean_data():
    for file in file_list:
        input_file = os.path.join(input_folder, file)
        output_file = os.path.join(output_folder, 'output_clean_' + file)
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
                        k += 1               
                    df.iat[i,j] = result
                else:
                    df.iat[i,j] = cell_value
                j += 1        
            i += 1

        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        print(datetime.now())
    return "Done clean_data"

def filter_data():
    filter_file = os.path.join(root_folder, data["filter_text_file"])
    df_filter = pd.read_excel(filter_file)
    filter_dict = df_filter.to_dict(orient='records')

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
                    ignore_case = re.compile(re.escape(item['Name']), re.IGNORECASE)
                    result = ignore_case.sub(filter_value, text_value)
                    text_value = result
                df.iat[i,j] = result
            i += 1

        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        print(datetime.now())
    return "Done filter_data" 

def filter_unit():
    filter_file = os.path.join(root_folder, data["filter_unit_file"])
    df_filter = pd.read_excel(filter_file)
    filter_dict = df_filter.to_dict(orient='records')

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
                    if text_value.lower().find(item['Name'].lower()) > -1:
                        result = filter_value
                        break
                df.at[i,j] = result
            i += 1
        print(df.shape)
        df.to_excel(output_file, index = False)
        shutil.move(input_file, archive_file)
        print(datetime.now())
    return "Done filter_unit" 

if __name__ == "__main__":
   main(sys.argv[1:])