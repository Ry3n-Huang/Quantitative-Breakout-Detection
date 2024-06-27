from sqlalchemy import create_engine
import pymysql
import pandas as pd
from datetime import datetime, timedelta
import time

def create_connection():
    return pymysql.connect(host='124.220.177.115',
                           user='hwh',
                           password='gtja20',
                           db='wind',
                           port=3306,
                           cursorclass=pymysql.cursors.DictCursor)

def get_price(stock_list, end_date, fields, count):
    start_time = time.time()
    conn = create_connection()
    field_mapping = {
        'volume': 'S_DQ_VOLUME',
        'money': 'S_DQ_AMOUNT',
        'close': 'S_DQ_ADJCLOSE'
    }
    sql_fields = ', '.join([f"{field_mapping[f]} AS {f}" for f in fields])
    start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=count)).strftime('%Y%m%d')

    sql = f"""
    SELECT TRADE_DT, {sql_fields}, S_INFO_WINDCODE
    FROM ASHAREEODPRICES
    WHERE S_INFO_WINDCODE IN ({','.join(['%s' for _ in stock_list])})
      AND TRADE_DT BETWEEN %s AND %s
    ORDER BY S_INFO_WINDCODE, TRADE_DT
    """

    with conn.cursor() as cursor:
        cursor.execute(sql, stock_list + [start_date, end_date])
        result = cursor.fetchall()

    conn.close()
    end_time = time.time()
    print(f"get_price took {end_time - start_time} seconds")

    if result:
        df = pd.DataFrame(result)
        df.set_index(['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
        df.index = df.index.set_levels([pd.to_datetime(df.index.levels[0]), df.index.levels[1]])
        df = df.apply(pd.to_numeric, errors='coerce', axis=1)  # Convert all data to numeric, coercing errors
        return df.unstack(level='S_INFO_WINDCODE')  # Pivot S_INFO_WINDCODE to columns for each field
    else:
        return pd.DataFrame()


def get_trade_days(start_date, end_date):
    """
    Fetches the list of trading days from the factordb database between start_date and end_date.

    Args:
        start_date (str): The start date in 'YYYYMMDD' format.
        end_date (str): The end date in 'YYYYMMDD' format.

    Returns:
        list: A list of trading days between the specified dates that are present in the database.
    """
    
    conn = create_connection()
    cursor = conn.cursor()

    
    sql = f"""
        SELECT DISTINCT TRADE_DT
        FROM ASHAREEODPRICES
        WHERE TRADE_DT BETWEEN %s AND %s
        ORDER BY TRADE_DT
    """
    cursor.execute(sql, (start_date, end_date))

    results = cursor.fetchall()

    # Convert results into a list of dates
    trade_days = [row['TRADE_DT'] for row in results]

    cursor.close()
    conn.close()

    return trade_days


#v2
def get_horizontal_plateau_length(stock_code, breakout_date, days_back=1500):
    # 获取股票的收盘价数据，从突破点往前推days_back天
    close_prices = get_price([stock_code], end_date=breakout_date, fields=['close'], count=days_back)['close'][stock_code]

    # 计算60日移动平均线
    MA_60 = close_prices.rolling(window=60).mean()

    # 初始化变量
    date_index = close_prices.index
    start_date = None
    over_limit_counter = 0

    # 从突破日开始向前检查，直到找到符合横盘开始条件的日期
    for i in range(len(close_prices)-1, -1, -1):
        price_ratio = close_prices.iloc[i] / MA_60.iloc[i]
        # # 打印基准信息，观测数据
        # print(f"Date: {date_index[i]}, Price: {close_prices.iloc[i]}, MA_60: {MA_60.iloc[i]}, Ratio: {price_ratio}")
        
        if price_ratio > 1.05 or price_ratio < 0.8:
            over_limit_counter += 1
        else:
            over_limit_counter = 0  # 重置计数器
        
        # 检查是否达到了横盘条件：超限5次
        if over_limit_counter == 6:
            start_date = date_index[i]
            break

    if start_date is not None:
        # 计算横盘持续的天数
        plateau_length = (pd.to_datetime(breakout_date) - start_date).days
        return plateau_length, start_date.strftime('%Y-%m-%d')
    else:
        return 0, None


def analyze_stock_data(csv_file_path):
    
    data = pd.read_csv(csv_file_path)
    
    data['Trade Date'] = data['Trade Date'].astype(str)

    results = []

    # Iterate through each row in the DataFrame
    for index, row in data.iterrows():
        stock_code = row['Stock Code']
        trade_date = row['Trade Date'].replace('-', '')  # Ensure the date format is 'YYYYMMDD'
        
        # Call the function with the current row's stock code and trade date
        length, start_date = get_horizontal_plateau_length(stock_code, trade_date)
        
        # Append the results in a new row for the results DataFrame
        results.append({
            'Stock Code': stock_code,
            'Breakout Date': trade_date,
            'Horizontal Plateau Length': length,
            'Plateau Start Date': start_date
        })
    
    # Convert the results list into a DataFrame
    results_df = pd.DataFrame(results)
    
    results_df.to_csv('2023_new_analysis_results.csv', index=False)
    print("Analysis complete, results saved to '2023_analysis_results.csv'.")


csv_file_path = '/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v3 t=5/2023.csv'
analyze_stock_data(csv_file_path)
