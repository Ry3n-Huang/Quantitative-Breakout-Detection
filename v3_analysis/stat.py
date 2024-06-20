import pandas as pd
from scipy.stats import ttest_1samp

# List of file paths and years
file_paths = [
    '2018.csv', '2019.csv', '2020.csv', '2021.csv', '2022.csv', '2023.csv'
]
years = [2018, 2019, 2020, 2021, 2022, 2023]

# Placeholder for combined data
all_data = pd.DataFrame()

# Function to calculate statistics and perform t-test
def calculate_stats(data, year):
    mean_cumulative = data['Cumulative Return'].mean()
    median_cumulative = data['Cumulative Return'].median()
    max_cumulative = data['Cumulative Return'].max()
    min_cumulative = data['Cumulative Return'].min()

    mean_excess = data['Excess Return'].mean()
    median_excess = data['Excess Return'].median()
    max_excess = data['Excess Return'].max()
    min_excess = data['Excess Return'].min()

    t_stat_cumulative, p_value_cumulative = ttest_1samp(data['Cumulative Return'].dropna(), 0)
    t_stat_excess, p_value_excess = ttest_1samp(data['Excess Return'].dropna(), 0)

    return {
        'year': year,
        'mean_cumulative': mean_cumulative,
        'median_cumulative': median_cumulative,
        'max_cumulative': max_cumulative,
        'min_cumulative': min_cumulative,
        'p_value_cumulative': p_value_cumulative,
        'mean_excess': mean_excess,
        'median_excess': median_excess,
        'max_excess': max_excess,
        'min_excess': min_excess,
        'p_value_excess': p_value_excess
    }

# Processing each file
results = []
for path, year in zip(file_paths, years):
    data = pd.read_csv(path)
    all_data = pd.concat([all_data, data[['Cumulative Return', 'Excess Return']]], ignore_index=True)
    results.append(calculate_stats(data, year))

# Save overall and yearly results to a txt file
with open('statistics_results.txt', 'w') as f:
    # Yearly results
    for result in results:
        f.write(f"{result['year']} Statistics:\n")
        f.write(f"Cumulative Return - Mean: {result['mean_cumulative']}, Median: {result['median_cumulative']}, Max: {result['max_cumulative']}, Min: {result['min_cumulative']}, P-Value: {result['p_value_cumulative']}\n")
        f.write(f"Excess Return - Mean: {result['mean_excess']}, Median: {result['median_excess']}, Max: {result['max_excess']}, Min: {result['min_excess']}, P-Value: {result['p_value_excess']}\n")
        f.write("\n")
