import pandas as pd

file_path = '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/github/Quantitative-Breakout-Detection/XGB/月宏观C.csv'
df = pd.read_csv(file_path)

print(df.head())

print(df.describe())

print(df['日期'].unique())
