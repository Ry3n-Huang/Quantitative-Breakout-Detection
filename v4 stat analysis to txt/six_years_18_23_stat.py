import pandas as pd
from scipy.stats import ttest_1samp

# List of file paths
file_paths = [
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v4 t=5 (correct CR)/modified_2018.csv', 
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v4 t=5 (correct CR)/modified_2019.csv', 
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v4 t=5 (correct CR)/modified_2020.csv', 
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v4 t=5 (correct CR)/modified_2021.csv', 
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v4 t=5 (correct CR)/modified_2022.csv', 
    '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v4 t=5 (correct CR)/modified_2023.csv'
]

# Placeholder for combined data
all_data = pd.DataFrame()

# Load and append data from each file
for path in file_paths:
    data = pd.read_csv(path)
    all_data = pd.concat([all_data, data[['Cumulative Return', 'Excess Return']]], ignore_index=True)

# Calculate statistics
mean_cumulative = all_data['Cumulative Return'].mean()
median_cumulative = all_data['Cumulative Return'].median()
max_cumulative = all_data['Cumulative Return'].max()
min_cumulative = all_data['Cumulative Return'].min()

mean_excess = all_data['Excess Return'].mean()
median_excess = all_data['Excess Return'].median()
max_excess = all_data['Excess Return'].max()
min_excess = all_data['Excess Return'].min()

# Perform a one-sample t-test against the hypothesis that the mean is 0
t_stat_cumulative, p_value_cumulative = ttest_1samp(all_data['Cumulative Return'].dropna(), 0)
t_stat_excess, p_value_excess = ttest_1samp(all_data['Excess Return'].dropna(), 0)

# Save results to a txt file
with open('statistics_results_6_years.txt', 'w') as f:
    f.write('Cumulative Return Statistics:\n')
    f.write(f'Mean: {mean_cumulative}\n')
    f.write(f'Median: {median_cumulative}\n')
    f.write(f'Max: {max_cumulative}\n')
    f.write(f'Min: {min_cumulative}\n')
    f.write(f'P-Value: {p_value_cumulative}\n\n')
    
    f.write('Excess Return Statistics:\n')
    f.write(f'Mean: {mean_excess}\n')
    f.write(f'Median: {median_excess}\n')
    f.write(f'Max: {max_excess}\n')
    f.write(f'Min: {min_excess}\n')
    f.write(f'P-Value: {p_value_excess}\n')
