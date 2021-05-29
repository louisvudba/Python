import pandas as pd

input_file = "I:\\Github\\Python\\Tmp\\Input\\aaa.xlsx"
output_file = "I:\\Github\\Python\\Tmp\\Input\\aaa.xlsx"

df = pd.read_excel(input_file, keep_default_na=False)

print('processing ' + file)
print(datetime.now())    

list1 = [f'{a}, {b}, {c} ==== {d}' 
    for a in df.Item if a != '' 
    for b in df.Type if b != '' 
    for c in df.Size if c != ''
    for d in df.ext if d != ''
    ]

dfRes = pd.DataFrame(data=list1)
dfRes.to_excel(output_file, index = False)

print(datetime.now())
print("Done crossjoin_data")