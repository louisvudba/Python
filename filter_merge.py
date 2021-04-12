import pandas as pd
import openpyxl

file_input = 'I:\\PythonRepo\\Manifest.xlsx'
# Create a test df
df = pd.read_excel(file_input)

# Create the list where we 'll capture the cells that appear for 1st time,
# add the 1st row and we start checking from 2nd row until end of df
startCells = [1]
for row in range(2,len(df)+1):
    if (df.loc[row-1,'Name'] != df.loc[row-2,'Name']):
        startCells.append(row)

book = openpyxl.load_workbook('I:\\PythonRepo\\test.xlsx') #Already existing workbook
writer = pd.ExcelWriter('examples/ex1.xlsx', engine='openpyxl') #Using openpyxl



writer = pd.ExcelWriter('I:\\PythonRepo\\test.xlsx', engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
df.to_excel(writer, sheet_name='Sheet1', index=False)
workbook = writer.book # pylint: disable=abstract-class-instantiated
worksheet = writer.sheets['Sheet1']
merge_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 2})


lastRow = len(df)

for row in startCells:
    try:
        endRow = startCells[startCells.index(row)+1]-1
        if row == endRow:
            worksheet.write(row, 0, df.loc[row-1,'Name'], merge_format)
        else:
            worksheet.merge_range(row, 0, endRow, 0, df.loc[row-1,'Name'], merge_format)
    except IndexError:
        if row == lastRow:
            worksheet.write(row, 0, df.loc[row-1,'Name'], merge_format)
        else:
            worksheet.merge_range(row, 0, lastRow, 0, df.loc[row-1,'Name'], merge_format)


writer.save()