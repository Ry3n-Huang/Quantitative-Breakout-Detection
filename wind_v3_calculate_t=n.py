import pandas as pd
import pymysql
from sqlalchemy import create_engine

def get_cumulative_return(trade_date, t, stock_code):
    db_url = 'mysql+pymysql://hwh:gtja20@124.220.177.115:3306/wind'
    engine = create_engine(db_url)
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


def main():

    df = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v4 t=5 (correct CR)/modified_2023.csv')
    
    intervals = [10, 20]
    
    results = []
    
    # 遍历数据集中的每行数据
    for index, row in df.iterrows():
        trade_date = row['Trade Date']
        stock_code = row['Stock Code']
        
        # 获取中证全指的累积收益
        index_returns = {t: get_index_cumulative_return(trade_date, t) for t in intervals}
        
        # 为每个时间间隔计算累积收益和超额收益
        for t in intervals:
            try:
                stock_return = get_cumulative_return(trade_date, t, stock_code)
                if index_returns[t] is not None and stock_return is not None:
                    excess_return = stock_return - index_returns[t]
                else:
                    excess_return = None
                results.append({
                    'Trade Date': trade_date,
                    'Stock Code': stock_code,
                    't': t,
                    'Cumulative Return': stock_return,
                    'Excess Return': excess_return
                })
            except Exception as e:
                print(f"Error processing {stock_code} on {trade_date} for t={t}: {str(e)}")
    
    results_df = pd.DataFrame(results)

    results_df.to_csv('2023_calculated_returns_with_excess.csv', index=False)

if __name__ == "__main__":
    main()
