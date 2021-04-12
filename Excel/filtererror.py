import pandas as pd
import numbers
from datetime import datetime
file_input = 'I:\\PythonRepo\\瑞恒-来赞达_MAWB688-20090102-175件.xlsx'
file_output = 'I:\\PythonRepo\\output.xlsx'
file_output1 = 'I:\\PythonRepo\\output1.xlsx'
filter_location = '%&+ ,-.0123456789:;ABCDEFGHIJKLMNOPQRSTUVWXYZ\][_`abcdefghijklmnopqrstuvwxyzáàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđÁÀẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬÉÈẺẼẸÊẾỀỂỄỆÍÌỈĨỊÓÒỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÚÙỦŨỤƯỨỪỬỮỰÝỲỶỸỴĐ'

print(datetime.now())
df = pd.read_excel(file_input)
#df.to_excel(file_output1)

#df_output = df.dataframe(df)
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
                if filter_location.find(text_value[k]) > 0 :
                    result += text_value[k]
                k += 1
            #print(text_value)
            #print(result)
            #print('===================')
            df.iat[i,j] = result
            #df.set_value(i,j,result)
        else:
            df.iat[i,j] = cell_value
        j += 1        
    i += 1

print(df.shape)
df.to_excel(file_output, index = False)
print(datetime.now())
