import pandas as pd
import numbers
import re
from datetime import datetime
file_input = 'I:\\PythonRepo\\source.xlsx'
file_output = 'I:\\PythonRepo\\output_col.xlsx'
filter_name = 'I:\\PythonRepo\\filtername.xlsx'

print(datetime.now())
df_filter = pd.read_excel(filter_name)
df = pd.read_excel(file_input)
#df.to_excel(file_output1)
#df_filter.set_index('Name')
#df_filter.transpose()
#filter_dict = df_filter['Name','Filter'].to_dict()
filter_dict = df_filter.to_dict(orient='records')
#print(filter_dict)
#for item in filter_dict:
   ## print(item['Name'], 'corresponds to', item['Filter'])

#df_output = df.dataframe(df)
r, c = df.shape
i = 0
j = 0

while i < 5:
    j = 0
    while j < c:        
        cell_value = df.iat[i,j]          
        if (pd.isna(cell_value)):
            text_value = ''
        else:
            text_value = str(cell_value)        
        result = ''
        for item in filter_dict:
            if pd.isna(item['Filter']):
                filter_value = ''
            else:
                filter_value = item['Filter']
            if text_value.lower().find(item['Name'].lower()) > -1:
                result = filter_value
                break
        #print(text_value)
        #print(result)
        #print('===================')
        df.at[i,j] = result
        #df.set_value(i,j,result)
        
        j += 1        
    i += 1

print(df.shape)
df.to_excel(file_output, index = False)
print(datetime.now())
