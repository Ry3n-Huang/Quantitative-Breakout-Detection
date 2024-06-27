import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


def set_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang HK']  # 使用苹方字体
    plt.rcParams['axes.unicode_minus'] = False  # 修正负号显示问题

set_chinese_font()

# 加载数据
merged_data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/t=5_totalyear_data.csv')

# 剔除横盘时间为0的数据
filtered_data = merged_data[merged_data['Horizontal Plateau Length'] > 0]

# 创建时间长度区间
bins = np.arange(0, filtered_data['Horizontal Plateau Length'].max() + 90, 90)  # 假设最大值加90确保包含所有数据
labels = [f'{int(b)}-{int(b+90)}' for b in bins[:-1]]
filtered_data['Length Interval'] = pd.cut(filtered_data['Horizontal Plateau Length'], bins=bins, labels=labels, right=False)

# 计算每个时间长度区间的平均收益率
interval_data = filtered_data.groupby('Length Interval')['Cumulative Return'].mean().reset_index()

# 将数据转换为热点图需要的格式
heatmap_data = interval_data.pivot_table(index='Cumulative Return', values='Length Interval', aggfunc=np.mean)

# 绘制热点图
plt.figure(figsize=(12, 8))
sns.heatmap(heatmap_data, annot=True, cmap='coolwarm', center=0)
# sns.heatmap(heatmap_data, annot=True, cmap='coolwarm', fmt=".2f")
plt.title('横盘时间长度区间与平均累计回报率的热点图')
plt.ylabel('横盘时间长度区间（天）')
plt.xlabel('平均累计回报率')
plt.show()
