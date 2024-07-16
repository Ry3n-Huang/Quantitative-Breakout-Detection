import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def set_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang HK']  # 使用苹方字体
    plt.rcParams['axes.unicode_minus'] = False  # 修正负号显示问题

set_chinese_font()

data_t5 = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/t=5_totalyear_data.csv')
data_combined = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/total_combined.csv')

# 将Trade Date和Breakout Date转换为日期格式
data_t5['Breakout Date'] = pd.to_datetime(data_t5['Breakout Date'])
data_combined['Trade Date'] = pd.to_datetime(data_combined['Trade Date'])

# 创建90天间隔的区间
bins = [0, 90, 180, 270, 360, 450, 540, 630, 720]
labels = ['0-90', '90-180', '180-270', '270-360', '360-450', '450-540', '540-630', '630-720']

# 函数用于处理每个t值的数据
def process_data(t_value):
    data_t = data_combined[data_combined['t'] == t_value].copy()
    data_t = pd.merge(data_t, data_t5[['Stock Code', 'Breakout Date', 'Horizontal Plateau Length']], left_on=['Stock Code', 'Trade Date'], right_on=['Stock Code', 'Breakout Date'], how='left')
    data_t['Interval'] = pd.cut(data_t['Horizontal Plateau Length'], bins=bins, labels=labels, right=False)
    return data_t.pivot_table(index='Interval', values='Cumulative Return', aggfunc='mean')

pivot_t5 = process_data(5)
pivot_t10 = process_data(10)
pivot_t20 = process_data(20)

pivot_t5 = pivot_t5 * 100
pivot_t10 = pivot_t10 * 100
pivot_t20 = pivot_t20 * 100
pivot_combined = pd.concat([pivot_t5, pivot_t10, pivot_t20], axis=1, keys=['t=5', 't=10', 't=20'])

plt.figure(figsize=(12, 8))
sns.heatmap(pivot_combined, annot=True, fmt=".2f", cmap="coolwarm", cbar_kws={'label': '平均累计回报率 (%)'})
plt.title('t天后不同横盘时间长度的平均累计收益率')
plt.xlabel('t值及平均累计收益率')
plt.ylabel('横盘时间长度区间 (天)')
plt.show()
