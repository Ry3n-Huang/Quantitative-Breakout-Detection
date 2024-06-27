import pandas as pd
import matplotlib.pyplot as plt

def set_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang HK']  # 使用苹方字体
    plt.rcParams['axes.unicode_minus'] = False  # 修正负号显示问题

set_chinese_font()

# Load the dataset
data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=n/2018/t=5_totalyear_data.csv')

# Convert 'Breakout Date' to datetime format for extracting the year
data['Year'] = pd.to_datetime(data['Breakout Date'], format='%Y%m%d').dt.year

# Filter data for years 2018-2023
filtered_data = data[data['Year'].between(2018, 2023)]

# Count occurrences of each industry per year
industry_counts = filtered_data.groupby(['Year', 'Industry Name']).size().reset_index(name='Counts')

# Sort data within each year by occurrence and get the top 7 industries
top_industries = industry_counts.groupby('Year').apply(lambda x: x.nlargest(7, 'Counts')).reset_index(drop=True)

# Add an "Others" category for each year
final_data = []
for year in range(2018, 2024):
    year_data = industry_counts[industry_counts['Year'] == year]
    top_year_data = top_industries[top_industries['Year'] == year]
    others_count = year_data[~year_data['Industry Name'].isin(top_year_data['Industry Name'])]['Counts'].sum()
    others_row = pd.DataFrame({'Year': [year], 'Industry Name': ['其他'], 'Counts': [others_count]})
    final_data.append(pd.concat([top_year_data, others_row]))

# Combine all years
final_data = pd.concat(final_data)

# Plotting
fig, axes = plt.subplots(2, 3, figsize=(15, 10))
colors = ['#C4E4FF', '#D895DA', '#7469B6', '#AD88C6', '#E1AFD1', '#FFE6E6', '#B0E0E6', '#FFDAB9']  # Colors for 7 industries and "Others"
year = 2018
for i, ax in enumerate(axes.flatten()):
    year_data = final_data[final_data['Year'] == year]
    ax.pie(year_data['Counts'], labels=year_data['Industry Name'], autopct='%1.1f%%', colors=colors[:len(year_data)], startangle=90)
    ax.set_title(f"行业分布 {year}")
    year += 1

plt.tight_layout()
plt.show()
