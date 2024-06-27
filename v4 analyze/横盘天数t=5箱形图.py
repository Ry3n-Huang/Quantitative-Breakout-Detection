import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def set_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang HK']  # 使用苹方字体
    plt.rcParams['axes.unicode_minus'] = False  # 修正负号显示问题

set_chinese_font()

# 加载数据
data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/t=5_totalyear_data.csv')  # 替换为实际的文件路径

# 排除横盘时间长度为0或者null值的数据
data = data[(data['Horizontal Plateau Length'] > 0) & data['Horizontal Plateau Length'].notnull()]

# 添加一个新的列来表示90天间隔的区间
bins = [0, 90, 180, 270, 360, 450, 540, 630, 720]
labels = ['0-90', '90-180', '180-270', '270-360', '360-450', '450-540', '540-630', '630-720']
data['Interval'] = pd.cut(data['Horizontal Plateau Length'], bins=bins, labels=labels, right=False)

# 绘制箱形图
plt.figure(figsize=(12, 6))
sns.boxplot(x='Interval', y='Cumulative Return', data=data)
plt.title('横盘时间长度的平均累计回报率的箱形图')
plt.xlabel('横盘时间长度区间 (天)')
plt.ylabel('累计回报率')
plt.show()
