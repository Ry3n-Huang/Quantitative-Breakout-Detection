import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_and_plot_market_values(file_path):
    # Load the data
    data = pd.read_csv(file_path)

    # Group data by '区间' and calculate the count, min, and max of 'Market Value' for each interval
    interval_stats = data.groupby('区间')['Market Value'].agg(['count', 'min', 'max']).reset_index()

    # Plotting the count of each interval
    plt.figure(figsize=(10, 6))
    sns.barplot(x='区间', y='count', data=interval_stats, palette='viridis')
    plt.title('Count of Entries by Interval')
    plt.ylabel('Count')
    plt.xlabel('Interval')
    plt.show()

    # Printing the min and max market values for each interval
    print("Market Value Statistics for Each Interval:")
    print(interval_stats[['区间', 'min', 'max']])

# Replace 'path_to_your_file.csv' with the path to your CSV file
file_path = '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/t=5 all data(2018-2023)/2023.csv'
analyze_and_plot_market_values(file_path)
