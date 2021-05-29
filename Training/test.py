import pandas as pd

df = pd.read_excel("I:\\Github\\Python\\Tmp\\aaa.xlsx", keep_default_na=False)
list1 = [f'{a}, {b}, {c}' for a in df.Item if a != '' for b in df.Type if b != '' for c in df.Size if c != '']
print(list1)