import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def set_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang HK']  # 使用苹方字体
    plt.rcParams['axes.unicode_minus'] = False  # 修正负号显示问题

set_chinese_font()

# Load the data
file_path = '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/total_combined.csv'
data = pd.read_csv(file_path)

# Ensure each Trade Date and Stock Code combination has the same value for 区间
unique_groups = data.groupby(['Trade Date', 'Stock Code'])['区间'].nunique()
if unique_groups.max() > 1:
    raise ValueError("发现相同的 Trade Date 和 Stock Code 组合有不同的区间值")

# Extract t=5 data as reference
reference_data = data[data['t'] == 5][['Trade Date', 'Stock Code', '区间']].rename(columns={'区间': 'Reference 区间'})

# Merge t=10 and t=20 data with reference data
merged_data = data.merge(reference_data, on=['Trade Date', 'Stock Code'], how='left')

# Fill missing 区间 values in t=10 and t=20 data
merged_data['区间'] = merged_data['区间'].combine_first(merged_data['Reference 区间'])

# Filter out t=5, 10, 20 data
filtered_data = merged_data[merged_data['t'].isin([5, 10, 20])]

# Create a pivot table to calculate mean cumulative returns for t=5, 10, 20
pivot_table_filtered = filtered_data.pivot_table(
    values='Cumulative Return',
    index='区间',
    columns='t',
    aggfunc='mean'
)

# Convert cumulative returns to percentage
pivot_table_filtered = pivot_table_filtered * 100

# Plotting the heatmap
plt.figure(figsize=(12, 8))
sns.heatmap(pivot_table_filtered, annot=True, cmap='YlGnBu', cbar=True, fmt='.2f')
plt.title('按市值区间和时间段划分的平均累积收益')
plt.xlabel('突破后时间 (天)')
plt.ylabel('市值区间')
plt.show()
