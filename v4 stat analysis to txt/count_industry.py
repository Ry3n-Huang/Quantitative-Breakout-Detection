import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from functools import reduce
import numpy as np

# 设置中文显示
def set_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang HK']  # 使用苹方字体
    plt.rcParams['axes.unicode_minus'] = False  # 修正负号显示问题

set_chinese_font()

def load_and_analyze_data(file_paths):
    dataframes = []
    yearly_counts = []

    # Load each file, calculate the industry counts and plot
    for year, path in enumerate(file_paths, start=2018):
        df = pd.read_csv(path)
        industry_count = df['Industry Name'].value_counts().rename_axis('Industry').reset_index(name=f'Count_{year}')
        dataframes.append(df)
        yearly_counts.append(industry_count)

        # Plotting each year's industry distribution
        plt.figure(figsize=(12, 6))
        plt.bar(industry_count['Industry'], industry_count[f'Count_{year}'], color='skyblue')
        plt.title(f'行业分布 {year}年')
        plt.xlabel('行业')
        plt.ylabel('数量')
        plt.xticks(rotation=35)
        plt.tight_layout()
        plt.savefig(f'Industry_Distribution_{year}.png')
        plt.close()

    # Combine all years counts for final analysis
    combined_counts = reduce(lambda left, right: pd.merge(left, right, on='Industry', how='outer'), yearly_counts)
    combined_counts.to_csv('combined_industry_counts.csv', index=False)

    # Generate cumulative plot for all years
    plot_cumulative_distribution(combined_counts)

    return combined_counts

def plot_cumulative_distribution(combined_counts):
    numeric_cols = combined_counts.select_dtypes(include=[np.number])  # 确保只处理数值列
    combined_counts['Total'] = numeric_cols.sum(axis=1)
    
    # Sort industries by total counts
    sorted_combined_counts = combined_counts.sort_values(by='Total', ascending=False)
    
    # Plotting the cumulative distribution
    plt.figure(figsize=(12, 6))
    plt.bar(sorted_combined_counts['Industry'], sorted_combined_counts['Total'], color='skyblue')
    plt.title('六年行业累计分布')
    plt.xlabel('行业')
    plt.ylabel('总数量')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('Cumulative_Industry_Distribution.png')
    plt.close()

# File paths
file_paths = [
    "/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v3 t=5/2018.csv",
    "/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v3 t=5/2019.csv",
    "/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v3 t=5/2020.csv",
    "/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v3 t=5/2021.csv",
    "/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v3 t=5/2022.csv",
    "/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v3 t=5/2023.csv"
]

# Adjust paths as necessary
combined_counts = load_and_analyze_data(file_paths)
print(combined_counts)
