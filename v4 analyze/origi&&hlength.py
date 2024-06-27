import pandas as pd

# 加载数据集
combined_data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/2018_combined.csv')
hori_length_data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/2018_hori_length.csv')

# 对combined_data中的'Trade Date'列重命名，以便与hori_length_data中的'Breakout Date'列对应
combined_data.rename(columns={'Trade Date': 'Breakout Date'}, inplace=True)

# 使用merge函数根据'Stock Code'和'Breakout Date'列合并两个数据表
merged_data = pd.merge(combined_data, hori_length_data, on=['Stock Code', 'Breakout Date'])

# 保存合并后的数据到新的CSV文件
merged_data.to_csv('2018_final.csv', index=False)