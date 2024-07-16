import pandas as pd

combined_data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/github/Quantitative-Breakout-Detection/v4 data/six_years_t=n_data.csv')
hori_length_data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/github/Quantitative-Breakout-Detection/v4 data/six_years_t=5_data_with_hlength.csv')

# 对combined_data中的'Trade Date'列重命名，以便与hori_length_data中的'Breakout Date'列对应
combined_data.rename(columns={'Trade Date': 'Breakout Date'}, inplace=True)

# 使用merge函数根据'Stock Code'和'Breakout Date'列合并两个数据表
merged_data = pd.merge(combined_data, hori_length_data, on=['Stock Code', 'Breakout Date'])

merged_data.to_csv('total_final.csv', index=False)
