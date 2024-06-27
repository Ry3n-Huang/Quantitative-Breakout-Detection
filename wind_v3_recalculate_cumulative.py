import pandas as pd
from sqlalchemy import create_engine

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

# Read the existing CSV
data = pd.read_csv('/Users/xuanming/Desktop/国泰君安/西部研报_横盘突破/GTJA/v3 t=5/2023.csv')

# Update the Cumulative Return and Excess Return
for index, row in data.iterrows():
    t = 5  # Set t based on your specific needs
    new_cumulative_return = get_cumulative_return(row['Trade Date'], t, row['Stock Code'])
    index_cumulative_return = get_index_cumulative_return(row['Trade Date'], t)
    new_excess_return = new_cumulative_return - index_cumulative_return

    data.at[index, 'Cumulative Return'] = new_cumulative_return
    data.at[index, 'Excess Return'] = new_excess_return

# Save the modified data to a new CSV
data.to_csv('modified_2023.csv', index=False)
