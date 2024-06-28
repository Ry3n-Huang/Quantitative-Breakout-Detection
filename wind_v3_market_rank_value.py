from sqlalchemy import create_engine
import pandas as pd

# 连接数据库
db_url = 'mysql+pymysql://hwh:gtja20@124.220.177.115:3306/wind'
engine = create_engine(db_url)

# 获取中证全指成分股
query_components = """
    SELECT S_CON_WINDCODE
    FROM AINDEXMEMBERS
    WHERE S_INFO_WINDCODE = '000985.CSI'
"""
components = pd.read_sql(query_components, engine)

# 获取成分股的市值数据
query_S_VAL_MVs = f"""
    SELECT S_INFO_WINDCODE, S_VAL_MV
    FROM ASHAREEODDERIVATIVEINDICATOR
    WHERE S_INFO_WINDCODE IN ({','.join("'" + components['S_CON_WINDCODE'] + "'")})
"""
S_VAL_MVs = pd.read_sql(query_S_VAL_MVs, engine)

# 排序市值数据
sorted_S_VAL_MVs = S_VAL_MVs['S_VAL_MV'].sort_values()

# 计算市值区间
n_bins = 5
bin_edges = pd.qcut(sorted_S_VAL_MVs, q=n_bins, retbins=True)[1]

# 构建区间列表并打印
bin_edges_list = [(bin_edges[i], bin_edges[i+1]) for i in range(len(bin_edges)-1)]
for i, (lower, upper) in enumerate(bin_edges_list):
    print(f"区间 {i+1}: {lower:.2f} - {upper:.2f}")

bin_edges_list
