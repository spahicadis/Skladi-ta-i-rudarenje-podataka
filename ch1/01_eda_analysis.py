import pandas as pd

url = r"../Traffic_Analysis_Novigrad_2026-08-20_2026-08-31.csv"

data = pd.read_csv(url, delimiter=',')

print(data.head())

print(data.shape)

print(data.isna().sum())

print(data.nunique())

print(data.dtypes)

for column in data:
    print(data[column].value_counts())
    input("...")

print(data.columns.values)
