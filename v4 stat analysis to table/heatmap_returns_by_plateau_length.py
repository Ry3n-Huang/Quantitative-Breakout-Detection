import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def set_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang HK']  # 使用苹方字体
    plt.rcParams['axes.unicode_minus'] = False  # 修正负号显示问题

set_chinese_font()


data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/2018_final.csv')

# 筛选出t=5的数据
data_t5 = data[data['t'] == 5]

# 排除横盘时间为0的数据
data_t5_filtered = data_t5[data_t5['Horizontal Plateau Length'] > 0]

# 创建横盘时间长度区间
bins = np.arange(0, data_t5_filtered['Horizontal Plateau Length'].max() + 60, 60)
labels = [f'{int(b)}-{int(b+60)}' for b in bins[:-1]]
data_t5_filtered['Length Interval'] = pd.cut(data_t5_filtered['Horizontal Plateau Length'], bins=bins, labels=labels, right=False)

# 计算每个区间的平均累计回报率
pivot_data = data_t5_filtered.pivot_table(index='Length Interval', values='Cumulative Return', aggfunc=np.mean)

plt.figure(figsize=(10, 6))
sns.heatmap(pivot_data, annot=True, cmap='coolwarm', center=0)
# sns.heatmap(pivot_data, annot=True, cmap='coolwarm', fmt=".2f")
plt.title('Heatmap of Cumulative Return by Horizontal Plateau Length Interval (t=5)')
plt.ylabel('Horizontal Plateau Length Interval (days)')
plt.xlabel('Average Cumulative Return')
plt.show()
