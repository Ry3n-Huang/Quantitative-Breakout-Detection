from sqlalchemy import create_engine
import pymysql
import pandas as pd
from datetime import datetime, timedelta

def create_connection():
    return pymysql.connect(host='124.220.177.115',
                           user='hwh',
                           password='gtja20',
                           db='factordb',
                           port=3306,
                           cursorclass=pymysql.cursors.DictCursor)


def get_all_securities(date):
    """
    Fetches distinct S_INFO_WINDCODE data from the BASICFACTORS_TABLE in the factordb database for the given date.

    Args:
        date (str): Trading date in 'YYYYMMDD' format.

    Returns:
        list: List of unique S_INFO_WINDCODEs.
    """

    
    conn = create_connection()
    cursor = conn.cursor()

    # retrieve distinct stock codes for a given date
    sql = f"""
        SELECT DISTINCT S_INFO_WINDCODE
        FROM BASICFACTORS_TABLE
        WHERE TRADE_DT >= %s - 2 AND TRADE_DT <= %s
    """
    cursor.execute(sql, (date, date))  # parameters to avoid SQL injection

    
    results = cursor.fetchall()

    # convert results into a list (results are dictionaries due to DictCursor)
    data_list = [row['S_INFO_WINDCODE'] for row in results]

    
    cursor.close()
    conn.close()

    return data_list



def get_price(stock_list, end_date, fields, count):
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
    FROM BASICFACTORS_TABLE
    WHERE S_INFO_WINDCODE IN ({','.join(['%s' for _ in stock_list])})
      AND TRADE_DT BETWEEN %s AND %s
    ORDER BY S_INFO_WINDCODE, TRADE_DT
    """

    with conn.cursor() as cursor:
        cursor.execute(sql, stock_list + [start_date, end_date])
        result = cursor.fetchall()

    conn.close()

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
        FROM BASICFACTORS_TABLE
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
    FROM BASICFACTORS_TABLE
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
    
    db_url = 'mysql+pymysql://hwh:gtja20@124.220.177.115:3306/factordb'
    engine = create_engine(db_url)
    
    
    query = f"""
    SELECT TRADE_DT, S_DQ_PCTCHANGE
    FROM BASICFACTORS_TABLE
    WHERE S_INFO_WINDCODE = '{stock_code}' AND TRADE_DT >= '{trade_date}'
    ORDER BY TRADE_DT
    LIMIT {t}
    """
    df = pd.read_sql(query, engine)
    
    # 确保S_DQ_PCTCHANGE列为数值类型
    df['S_DQ_PCTCHANGE'] = pd.to_numeric(df['S_DQ_PCTCHANGE'], errors='coerce')

    cumulative_return = (df['S_DQ_PCTCHANGE'] / 100 + 1).prod() - 1
    
    # 将numpy.float64转换为内置float类型
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

def get_market_value(trade_date, stock_codes):

    db_url = 'mysql+pymysql://hwh:gtja20@124.220.177.115:3306/wind'
    engine = create_engine(db_url)

    results = []

    for stock_code in stock_codes:
        query = f"""
        SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_MV
        FROM ASHAREEODDERIVATIVEINDICATOR
        WHERE S_INFO_WINDCODE = '{stock_code}' AND TRADE_DT = '{trade_date}'
        """
        df = pd.read_sql(query, engine)
        
        if not df.empty:
            
            results.append({'Trade Date': trade_date, 'Stock Code': stock_code, 'Market Value': df['S_DQ_MV'].values[0]})
        else:
            
            results.append({'Trade Date': trade_date, 'Stock Code': stock_code, 'Market Value': None, 'Error': 'No data found'})

    results_df = pd.DataFrame(results)
    
    return results_df

def get_index_cumulative_return(trade_date, t):
    
    db_url = 'mysql+pymysql://hwh:gtja20@124.220.177.115:3306/wind'
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



def get_industry_name(stock_code):
    
    db_url = 'mysql+pymysql://hwh:gtja20@124.220.177.115:3306/wind'
    engine = create_engine(db_url)
    
    query_sw_industry = f"""
    SELECT SW_IND_CODE
    FROM ASHARESWINDUSTRIESCLASS
    WHERE S_INFO_WINDCODE = '{stock_code}'
    """
    df_sw_industry = pd.read_sql(query_sw_industry, engine, params={"stock_code": stock_code})
    
    if df_sw_industry.empty:
        print(f"未找到股票代码 {stock_code} 的行业代码")
        return None
    
    sw_ind_code = df_sw_industry['SW_IND_CODE'].values[0]
    # 将行业代码补齐到16位
    sw_ind_code_padded = sw_ind_code.ljust(16, '0')
    
    # 查询SW行业代码对应的行业名称
    query_industry_name = f"""
    SELECT INDUSTRIESNAME
    FROM ASHAREINDUSTRIESCODE
    WHERE INDUSTRIESCODE = '{sw_ind_code_padded}'
    """
    df_industry_name = pd.read_sql(query_industry_name, engine, params={"sw_ind_code": sw_ind_code})
    
    if df_industry_name.empty:
        print(f"未找到行业代码 {sw_ind_code} 对应的行业名称")
        return None
    
    industry_name = df_industry_name['INDUSTRIESNAME'].values[0]
    print(f"股票代码 {stock_code} 的行业名称为 {industry_name}")
    return industry_name



def main():
    start_date = '20210101'
    end_date = '20211231'
    t = 5
    date_list = get_trade_days(start_date=start_date, end_date=end_date)
    
    # 创建一个空的DataFrame用于存储结果
    all_results = pd.DataFrame()
    
    for date in date_list:
        feasible_stocks = get_feasible_stocks(date)
        if feasible_stocks:
            print(f"在日期 {date} 符合条件的股票代码有: {feasible_stocks}")
            # 获取个股的累计收益率
            returns_df = calculate_returns_for_stocks(date, t, feasible_stocks)
            # 获取个股的市值
            market_values_df = get_market_value(date, feasible_stocks)
            # 获取中证全指的累计收益率
            try:
                index_cumulative_return = get_index_cumulative_return(date, t)
            except ValueError as e:
                print(f"中证全指在日期 {date} 数据不足 {t} 个交易日")
                continue

            # 合并两个DataFrame
            combined_df = pd.merge(returns_df, market_values_df, on=['Trade Date', 'Stock Code'], how='outer')

            # 计算超额收益率
            combined_df['Excess Return'] = combined_df['Cumulative Return'] - index_cumulative_return

            # 获取每个股票的行业名称
            combined_df['Industry Name'] = combined_df['Stock Code'].apply(get_industry_name)

            # 将结果追加到总的DataFrame中
            all_results = pd.concat([all_results, combined_df], ignore_index=True)
        else:
            print(f"在日期 {date} 没有符合条件的股票")
    
    # 保存结果为CSV文件
    # ！！！每次调用要自定义CSV文件名！！！
    all_results.to_csv('results.csv', index=False)
    print("结果已保存到 results.csv")


if __name__ == "__main__":
    main()


