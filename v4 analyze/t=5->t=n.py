import pandas as pd

# 加载数据集
data_t5 = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/combined_modified_data.csv')
data_tn = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/combined_calculated_returns_with_excess.csv')

# 为data_t5数据集添加 't' 列
data_t5['t'] = 5

# 假设data_tn数据集已经包含名为 't' 的列，并包括了时间窗口10和20的数据
# 如果data_tn没有't'列，你需要根据数据适当创建这一列

# 合并数据集
combined_data = pd.concat([data_t5, data_tn], ignore_index=True)

# 查看合并后的数据
print(combined_data.head())

# 保存合并后的数据到新的CSV文件
combined_data.to_csv('total_combined.csv', index=False)
