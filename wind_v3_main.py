from sqlalchemy import create_engine
import pymysql
import pandas as pd
from datetime import datetime, timedelta
import time

def create_connection():
    return pymysql.connect(host='0.0.0.0',
                           user='Username',
                           password='Password',
                           db='dbname',
                           port=3306,
                           cursorclass=pymysql.cursors.DictCursor)


def get_all_securities(date):
    """
    Fetches distinct S_INFO_WINDCODE data from the ASHAREEODPRICES in the factordb database for the given date.

    Args:
        date (str): Trading date in 'YYYYMMDD' format.

    Returns:
        list: List of unique S_INFO_WINDCODEs.
    """
    start_time = time.time()
    
    conn = create_connection()
    cursor = conn.cursor()

    # retrieve distinct stock codes for a given date
    sql = f"""
        SELECT DISTINCT S_INFO_WINDCODE
        FROM ASHAREEODPRICES
        WHERE TRADE_DT >= %s - 2 AND TRADE_DT <= %s
    """
    cursor.execute(sql, (date, date))  # parameters to avoid SQL injection

    
    results = cursor.fetchall()

    # convert results into a list (results are dictionaries due to DictCursor)
    data_list = [row['S_INFO_WINDCODE'] for row in results]

    
    cursor.close()
    conn.close()
    end_time = time.time()
    print(f"get_all_securities took {end_time - start_time} seconds")

    return data_list



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


def get_bars(stock_list, count, unit='1w', fields=['TRADE_DT', 'S_DQ_ADJCLOSE'], include_now=True, end_dt=None, df=True):
    conn = create_connection()
    cursor = conn.cursor()

    # calculate the start date for the query based on the unit and count
    days_count = count * 7 if unit == '1w' else count
    start_date = (datetime.strptime(end_dt, '%Y%m%d') - timedelta(days=days_count)).strftime('%Y%m%d')

    placeholders = ', '.join(['%s' for _ in stock_list])
    sql = f"""
    SELECT S_INFO_WINDCODE as 'level_0', TRADE_DT as 'date', S_DQ_ADJCLOSE as 'close'
    FROM ASHAREEODPRICES
    WHERE S_INFO_WINDCODE IN ({placeholders})
      AND TRADE_DT BETWEEN %s AND %s
    ORDER BY S_INFO_WINDCODE, TRADE_DT DESC
    """
    cursor.execute(sql, stock_list + [start_date, end_dt])
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    if results:
        df = pd.DataFrame(results)
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values(by=['level_0', 'date'], inplace=True)

        # set 'date' as the index before resampling
        df.set_index('date', inplace=True)

        if unit == '1w':
            # now 'date' is the sole index, we can resample weekly
            df = df.groupby('level_0').resample('W').last().drop(columns=['level_0'])

        return df.reset_index() if df is not None and not df.empty else pd.DataFrame()
    return pd.DataFrame()


# 获取指定日期符合箱体突破要求的个股
def get_feasible_stocks(date):
    # print(f"Checking stocks for date: {date}")
    
    # 获取所有股票
    stock_list = get_all_securities(date)
    
    # 单日成交量是前5日平均成交量的两倍以上
    if stock_list:
        volume = get_price(stock_list, end_date=date, fields=['volume'], count=6)['volume']
        # print(f"Volume data: {volume}")
        indice = volume.iloc[-1, :] > 2 * volume[:-1].mean()
        stock_list = indice[indice == 1].index.tolist()
    
    # 单日成交额大于2亿
    if stock_list:
        money = get_price(stock_list, end_date=date, fields=['money'], count=1)['money'].iloc[-1, :]
        stock_list = money[money > 200000].index.tolist()
    
    # 收盘价站稳60日均线
    if stock_list:
        close = get_price(stock_list, end_date=date, fields=['close'], count=60)['close']
        indice = close.iloc[-1, :] > close.mean()
        stock_list = indice[indice == 1].index.tolist()
        
    
    # 近40日价格波幅超出N日均价的(0.8,1.05)倍低于5次
    if stock_list:
        N = 60
        close = get_price(stock_list, end_date=date, fields=['close'], count=100)['close']
        MA_N = close[-N:].mean()
        indice = ((close[-40:] > 1.05 * MA_N) | (close[-40:] < 0.8 * MA_N)).sum()
        stock_list = indice[indice < 5].index.tolist()
    
    # 排除阶段性高位股票（最新价格处于近一年价格分位数90%以下）
    if stock_list:
        close = get_price(stock_list, end_date=date, fields=['close'], count=250)['close']
        rank = close.rank(pct=True).iloc[-1, :]
        stock_list = rank[rank < 0.9].index.tolist()
    
    # 长周期均线有走平并转向上趋势（250日均线方向向上）
    if stock_list:
        close = get_price(stock_list, end_date=date, fields=['close'], count=300)['close']
        MA_250 = close.rolling(window=250).mean()
        MA_250.iloc[-1, :][MA_250.iloc[-1, :] > 0].index.tolist()
        # print(f"Stocks after volume filter: {stock_list}")
    
    # 时间跨度至少N=60个交易日（横盘区间内最高价距离最低价的幅度小于30%）
    if stock_list:
        N = 60
        close = get_price(stock_list, end_date=date, fields=['close'], count=N+3)['close'][:-3]
        rate = close.max() / close.min() - 1
        stock_list = rate[rate < 0.3].index.tolist()
        # 记录平台最高价
        high_ser = close[stock_list].max()
        # print(f"Stocks after volume filter: {stock_list}")
        
    # 收盘价高于平台上沿2%以上
    if stock_list:
        close = get_price(stock_list, end_date=date, fields=['close'], count=1)['close'].iloc[-1, :]
        rate = close / high_ser - 1
        stock_list = rate[rate > 0.02].index.tolist()
        # print(f"Stocks after volume filter: {stock_list}")
    
    # 日线连续几根阳线突破平台
    if stock_list:
        K = 2
        close = get_price(stock_list, end_date=date, fields=['close'], count=K+1)['close']
        # 连续K根阳线
        con1 = (close.diff().dropna(how='all') > 0).sum()
        # 突破平台最高价
        con2 = (close[1:] > high_ser[stock_list]).sum()
        stock_list = con1[(con1 == K) & (con2 == K)].index.tolist()
        # print(f"Stocks after volume filter 阳线: {stock_list}")

    
    # 周级别K线同样给出进场信号
    if stock_list:
        # 周线数据
        close = get_bars(stock_list, count=80, unit='1w',fields=['date','close'], include_now=True, end_dt=date, df=True).reset_index()
        close = pd.pivot_table(close, values='close', index='date', columns='level_0')
        # 周线收盘价站稳60周均线
        indice = close.iloc[-1, :] > close[-60:].mean()
        stock_list = indice[indice == 1].index.tolist()
        
    return stock_list

def get_cumulative_return(trade_date, t, stock_code):

    db_url = 'database_info'
    engine = create_engine(db_url)

    # 涨跌幅需手动计算（依据复权收盘价）
    query = f"""
    SELECT TRADE_DT, S_DQ_ADJCLOSE, S_DQ_ADJPRECLOSE
    FROM ASHAREEODPRICES
    WHERE S_INFO_WINDCODE = '{stock_code}' AND TRADE_DT >= '{trade_date}'
    ORDER BY TRADE_DT
    LIMIT {t}
    """
    df = pd.read_sql(query, engine)
    df['ADJ_PCT_CHANGE'] = (df['S_DQ_ADJCLOSE'] / df['S_DQ_ADJPRECLOSE'] - 1) * 100
    cumulative_return = ((df['ADJ_PCT_CHANGE'] / 100 + 1).prod() - 1)
    return float(cumulative_return)

def calculate_returns_for_stocks(trade_date, t, stock_codes):
    results = []
    
    for stock_code in stock_codes:
        try:
            cumulative_return = get_cumulative_return(trade_date, t, stock_code)
            results.append({'Trade Date': trade_date, 'Stock Code': stock_code, 'Cumulative Return': cumulative_return})
        except ValueError as e:
            results.append({'Trade Date': trade_date, 'Stock Code': stock_code, 'Cumulative Return': None, 'Error': str(e)})
    
    return pd.DataFrame(results)


def get_stock_rank(date, stock_list):
    db_url = 'database_info'
    engine = create_engine(db_url)
    
    # 获取中证全指成分股
    query_components = f"""
        SELECT S_CON_WINDCODE
        FROM AINDEXMEMBERS
        WHERE S_INFO_WINDCODE = '000985.CSI'
    """
    component_stocks = pd.read_sql(query_components, engine)['S_CON_WINDCODE'].tolist()

    # 获取成分股市值数据
    component_stocks_str = "', '".join(component_stocks)
    query_market_cap = f"""
        SELECT S_INFO_WINDCODE, S_VAL_MV
        FROM ASHAREEODDERIVATIVEINDICATOR
        WHERE TRADE_DT = '{date}'
        AND S_INFO_WINDCODE IN ('{component_stocks_str}')
    """
    market_cap_df = pd.read_sql(query_market_cap, engine)
    
    # 计算市值排名和区间
    market_cap_df['rank'] = pd.qcut(market_cap_df['S_VAL_MV'], 5, labels=False) + 1

    # 筛选特定股票的市值区间
    stock_list_str = "', '".join(stock_list)
    query_stock_list = f"""
        SELECT S_INFO_WINDCODE, S_VAL_MV
        FROM ASHAREEODDERIVATIVEINDICATOR
        WHERE TRADE_DT = '{date}'
        AND S_INFO_WINDCODE IN ('{stock_list_str}')
    """
    stock_list_df = pd.read_sql(query_stock_list, engine)

    result_df = stock_list_df.merge(market_cap_df[['S_INFO_WINDCODE', 'rank']], on='S_INFO_WINDCODE', how='left')
    result_df = result_df.rename(columns={'rank': '区间', 'S_INFO_WINDCODE': 'Stock Code', 'S_VAL_MV': 'Market Value'})

    result_df['Trade Date'] = date
    return result_df


def get_index_cumulative_return(trade_date, t):
    
    db_url = 'database_info'
    engine = create_engine(db_url)
    
    
    query = f"""
    SELECT TRADE_DT, S_DQ_PCTCHANGE
    FROM AINDEXEODPRICES
    WHERE S_INFO_WINDCODE = '000985.CSI' AND TRADE_DT >= '{trade_date}'
    ORDER BY TRADE_DT
    LIMIT {t}
    """
    df = pd.read_sql(query, engine)
    
    if len(df) < t:
        raise ValueError(f"数据不足{t}个交易日")
    
    df['S_DQ_PCTCHANGE'] = pd.to_numeric(df['S_DQ_PCTCHANGE'], errors='coerce')

    cumulative_return = (df['S_DQ_PCTCHANGE'] / 100 + 1).prod() - 1
    
    return cumulative_return


def get_industry_name(row):
    stock_code = row['Stock Code']
    date = row['Trade Date']
    
    db_url = 'database_info'
    engine = create_engine(db_url)
    
    # 查询股票对应的行业代码，确保date在ENTRY_DT和REMOVE_DT之间或者REMOVE_DT为NULL
    query_sw_industry = f"""
    SELECT SW_IND_CODE
    FROM ASHARESWINDUSTRIESCLASS
    WHERE S_INFO_WINDCODE = '{stock_code}'
    AND ENTRY_DT <= '{date}'
    AND (REMOVE_DT IS NULL OR REMOVE_DT >= '{date}')
    """
    df_sw_industry = pd.read_sql(query_sw_industry, engine)
    
    if df_sw_industry.empty:
        print(f"未找到股票代码 {stock_code} 在日期 {date} 的行业代码")
        return None
    
    # 一级只看前四位
    sw_ind_code = df_sw_industry['SW_IND_CODE'].values[0][:4]

    print(f'此时的indcode前四位是 {sw_ind_code} ')

    # 将行业代码补齐到16位
    sw_ind_code_padded = sw_ind_code.ljust(16, '0')
    
    # 查询SW行业代码对应的行业名称
    query_industry_name = f"""
    SELECT INDUSTRIESNAME
    FROM ASHAREINDUSTRIESCODE
    WHERE INDUSTRIESCODE = '{sw_ind_code_padded}'
    """
    df_industry_name = pd.read_sql(query_industry_name, engine)
    
    if df_industry_name.empty:
        print(f"未找到行业代码 {sw_ind_code} 对应的行业名称")
        return None
    
    industry_name = df_industry_name['INDUSTRIESNAME'].values[0]
    print(f"股票代码 {stock_code} 在日期 {date} 的行业名称为 {industry_name}")
    return industry_name


def filter_csi_all_share_stocks(stock_list):
    db_url = 'database_info'
    engine = create_engine(db_url)
    # 打印初始的stock_list
    print(f"初始的stock_list: {stock_list}")
    
    filtered_stock_list = []
    removed_stocks = []

    for stock in stock_list:
        query = f"""
        SELECT S_CON_WINDCODE
        FROM AINDEXMEMBERS
        WHERE S_INFO_WINDCODE = '000985.CSI'
        AND S_CON_WINDCODE = '{stock}'
        """
        df_result = pd.read_sql(query, engine)
        
        if df_result.empty:
            print(f"股票 {stock} 不在中证全指成分股中")
            removed_stocks.append(stock)
        else:
            print(f"股票 {stock} 在中证全指成分股中")
            filtered_stock_list.append(stock)
    
    # 打印不在中证全指成分股中的股票
    if removed_stocks:
        print(f"不在中证全指成分股中的股票: {removed_stocks}")
    else:
        print("所有股票都在中证全指成分股中")
    
    # 打印过滤后的stock_list
    print(f"过滤后的stock_list: {filtered_stock_list}")
    
    return filtered_stock_list


def main():
    # format: YYYYMMDD
    start_date = '20220107'
    print(type(start_date))
    end_date = '20220108'
    # t means days after stocks break out
    t = 5
    
    date_list = get_trade_days(start_date=start_date, end_date=end_date)
    
    # 创建一个空的DataFrame用于存储结果
    all_results = pd.DataFrame()
    
    for date in date_list:
        print(f"Processing date: {date}")  # 添加调试信息
        feasible_stocks = get_feasible_stocks(date)
        print(f"Feasible stocks for {date}: {feasible_stocks}")  # 添加调试信息
        csi_stocks = filter_csi_all_share_stocks(feasible_stocks)
        print(f"CSI stocks for {date}: {csi_stocks}")  # 添加调试信息
        if csi_stocks:
            print(f"在日期 {date} 符合条件的股票代码有: {csi_stocks}")
            # 获取个股的累计收益率
            returns_df = calculate_returns_for_stocks(date, t, csi_stocks)
            returns_df['Trade Date'] = date  # 确保包含Trade Date列
            print(f"Returns DataFrame for {date}:\n{returns_df}")  # 添加调试信息
            
            # 获取个股的市值及区间
            stock_rank_df = get_stock_rank(date, csi_stocks)
            print(f"Stock Rank DataFrame for {date}:\n{stock_rank_df}")  # 添加调试信息

            # 获取中证全指的累计收益率
            try:
                index_cumulative_return = get_index_cumulative_return(date, t)
                print(f"Index cumulative return for {date}: {index_cumulative_return}")  # 添加调试信息
            except ValueError as e:
                print(f"中证全指在日期 {date} 数据不足 {t} 个交易日")
                continue

            # 合并两个DataFrame
            combined_df = pd.merge(returns_df, stock_rank_df, on=['Trade Date', 'Stock Code'], how='outer')
            print(f"Combined DataFrame for {date}:\n{combined_df}")  # 添加调试信息

            # 计算超额收益率
            combined_df['Excess Return'] = combined_df['Cumulative Return'] - index_cumulative_return

            # 获取每个股票的行业名称
            combined_df['Industry Name'] = combined_df.apply(get_industry_name, axis=1)
            print(f"Final DataFrame for {date}:\n{combined_df}")  # 添加调试信息

            # 将结果追加到总的DataFrame中
            all_results = pd.concat([all_results, combined_df], ignore_index=True)
        else:
            print(f"在日期 {date} 没有符合条件的股票")
    
    all_results.to_csv('test_results.csv', index=False)
    print("结果已保存到 test_results.csv")

if __name__ == "__main__":
    main()




