
import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy import stats
import matplotlib.pyplot as plt

# 自定义文件路径
file_paths = [
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/V2_2018-2022/t=5/2018_results.csv',
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/V2_2018-2022/t=5/2019_results.csv',
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/V2_2018-2022/t=5/2020_results.csv',
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/V2_2018-2022/t=5/2021_results.csv',
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/V2_2018-2022/t=5/2022_results.csv'
]

dfs = [pd.read_csv(file) for file in file_paths]
data = pd.concat(dfs, ignore_index=True)


required_columns = ['Trade Date', 'Stock Code', 'Cumulative Return', 'Market Value', 'Excess Return', 'Industry Name']
data.columns = required_columns


data['Trade Date'] = pd.to_datetime(data['Trade Date'].astype(str), format='%Y%m%d')


data['Year'] = data['Trade Date'].dt.year
data['Month'] = data['Trade Date'].dt.month

data = data.dropna()


data['Market Value'] = data['Market Value'].astype(float)
data['Cumulative Return'] = data['Cumulative Return'].astype(float)
data['Excess Return'] = data['Excess Return'].astype(float)
data['Year'] = data['Year'].astype(int)
data['Month'] = data['Month'].astype(int)


industry_stats = data.groupby('Industry Name')[['Cumulative Return', 'Excess Return']].describe()

# 对行业统计按count从大到小排序
industry_counts = data['Industry Name'].value_counts()
sorted_industry_stats = industry_stats.reindex(industry_counts.index)

year_stats = data.groupby('Year')[['Cumulative Return', 'Excess Return']].describe()
month_stats = data.groupby('Month')[['Cumulative Return', 'Excess Return']].describe()

# 将市值分为五个区间，并进行描述性统计
data['Market Value Quintile'] = pd.qcut(data['Market Value'], 5, labels=["Quintile 1", "Quintile 2", "Quintile 3", "Quintile 4", "Quintile 5"])
market_value_stats = data.groupby('Market Value Quintile')[['Cumulative Return', 'Excess Return']].describe()

# 获取市值五个区间的范围
market_value_ranges = pd.qcut(data['Market Value'], 5, labels=["Quintile 1", "Quintile 2", "Quintile 3", "Quintile 4", "Quintile 5"]).unique()

# 将汇总统计信息写入文本文件
with open('summary_statistics.txt', 'w') as f:
    f.write("Sorted Industry Statistics:\n")
    f.write(sorted_industry_stats.to_string())
    f.write("\n\nYear Statistics:\n")
    f.write(year_stats.to_string())
    f.write("\n\nMonth Statistics:\n")
    f.write(month_stats.to_string())
    f.write("\n\nMarket Value Quintile Statistics:\n")
    f.write(market_value_stats.to_string())
    f.write("\n\nMarket Value Quintile Ranges:\n")
    for idx, range_ in enumerate(market_value_ranges):
        f.write(f"{range_}: {data[data['Market Value Quintile'] == range_]['Market Value'].min()} - {data[data['Market Value Quintile'] == range_]['Market Value'].max()}\n")
    f.write("\n\nMissing values:\n")
    f.write(data.isnull().sum().to_string())
    f.write("\n\nData types:\n")
    f.write(data.dtypes.to_string())

# t=5累计收益的箱形图按年份区分
plt.figure(figsize=(12, 8))
boxplot = data.boxplot(column='Cumulative Return', by='Year', grid=False, patch_artist=True)

colors = ['skyblue', 'lightgreen', 'lightyellow', 'lightpink', 'lightgray']
for patch, color in zip(boxplot.artists, colors):
    patch.set_facecolor(color)

plt.title('Cumulative Return at t=5 by Year')
plt.suptitle('')
plt.xlabel('Year')
plt.ylabel('Cumulative Return')
plt.show()


