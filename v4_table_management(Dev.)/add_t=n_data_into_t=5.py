import pandas as pd

# 加载数据集
data_t5 = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/combined_modified_data.csv')
data_tn = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/combined_calculated_returns_with_excess.csv')

# 为data_t5数据集添加 't' 列，data_tn必有 't' 列
data_t5['t'] = 5

# 合并数据集
combined_data = pd.concat([data_t5, data_tn], ignore_index=True)

print(combined_data.head())

combined_data.to_csv('total_combined.csv', index=False)
