import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def set_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang HK']  # 使用苹方字体
    plt.rcParams['axes.unicode_minus'] = False  # 修正负号显示问题

set_chinese_font()

combined_data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/github/Quantitative-Breakout-Detection/v4 data/six_years_t=n_data.csv')  # 替换为实际的文件路径

# 筛选出 t=5, 10, 20 的数据
filtered_data = combined_data[combined_data['t'].isin([5, 10, 20])]

# 计算每年不同t值下的平均累计回报和超额回报
mean_values = filtered_data.groupby(['Year', 't']).agg({
    'Cumulative Return': 'mean',
    'Excess Return': 'mean'
}).reset_index()

# 转换为百分数格式
mean_values['Cumulative Return'] = mean_values['Cumulative Return'] * 100
mean_values['Excess Return'] = mean_values['Excess Return'] * 100

cumulative_return_pivot = mean_values.pivot(index='Year', columns='t', values='Cumulative Return')
excess_return_pivot = mean_values.pivot(index='Year', columns='t', values='Excess Return')

plt.figure(figsize=(12, 6))
sns.heatmap(cumulative_return_pivot, annot=True, fmt=".3f", cmap="YlGnBu")
plt.title('平均累计收益')
plt.show()

plt.figure(figsize=(12, 6))
sns.heatmap(excess_return_pivot, annot=True, fmt=".3f", cmap="YlGnBu")
plt.title('平均超额收益')
plt.show()


print(mean_values)
