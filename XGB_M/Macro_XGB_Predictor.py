import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import RandomOverSampler
import xgboost as xgb
from sklearn.metrics import accuracy_score
import shap
import matplotlib.pyplot as plt

# # 本文用xgp默认参数值
# xgb_model = xgb.XGBClassifier(
#     eval_metric='logloss',
#     use_label_encoder=False,
#     learning_rate=0.1,
#     n_estimators=150,
#     max_depth=5,
#     subsample=0.8,
#     colsample_bytree=0.8
# )


def set_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang HK']
    plt.rcParams['axes.unicode_minus'] = False

set_chinese_font()

def load_data(filepath, encoding='utf-8'):
    try:
        df = pd.read_csv(filepath, encoding=encoding)
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding='gbk')  # 尝试使用gbk编码加载
    return df

def preprocess_data(df):
    df.drop(['序号'], axis=1, errors='ignore', inplace=True)
    df['日期'] = pd.to_datetime(df['日期'], format='%Y-%m-%d', errors='coerce')
    df.set_index('日期', inplace=True)
    df = df[df.index.year >= 1991]

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
    
    # 去除含缺失值的那一行
    df.dropna(subset=df.columns.difference(['上证综合指数']), inplace=True, how='any')

    if '上证综合指数' in df.columns:
        # 使用每个月的第一天的数据
        monthly_first = df['上证综合指数'].resample('MS').first()
        monthly_next_first = monthly_first.shift(-1)  # 下一个月的第一天

        # 计算月度涨跌
        monthly_diff = monthly_next_first - monthly_first
        monthly_up_down = monthly_diff.apply(lambda x: 1 if x > 0 else 0)
        df['指数月度涨跌'] = monthly_up_down.reindex(df.index, method='ffill')

    X = df.drop(['指数月度涨跌', '上证综合指数'], axis=1, errors='ignore')
    y = df['指数月度涨跌']

    if y.isna().any():
        print("NaN detected in y after processing:", y[y.isna()])
        raise ValueError("y contains NaN values after preprocessing.")
    return X, y




def prepare_data(X, y):
    if X.empty or y.empty:
        raise ValueError("Input data X or y is empty.")
    feature_names = X.columns.tolist()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train_scaled = pd.DataFrame(scaler.fit_transform(X_train), columns=feature_names)
    X_test_scaled = pd.DataFrame(scaler.transform(X_test), columns=feature_names)
    ros = RandomOverSampler(random_state=42)
    X_resampled, y_resampled = ros.fit_resample(X_train_scaled, y_train)
    return X_resampled, y_resampled, X_test_scaled, y_test, feature_names

def train_model(X_train, y_train, X_test, y_test):
    xgb_model = xgb.XGBClassifier(eval_metric='logloss', use_label_encoder=False, n_estimators=150, max_depth=5)
    xgb_model.fit(X_train, y_train)
    y_pred = xgb_model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    return xgb_model, accuracy

def analyze_model(xgb_model, X_test, feature_names):
    explainer = shap.Explainer(xgb_model)
    shap_values = explainer(X_test)
    shap.summary_plot(shap_values, feature_names=feature_names, plot_type="bar")  # 确保传递特征名称

def load_and_clean_data(filepath, encoding='utf-8'):
    df = pd.read_csv(filepath, encoding=encoding)
    # 确认数据中的日期列不包含非日期的行
    df = df[df['日期'].apply(lambda x: isinstance(x, str) and x.replace('.', '', 1).isdigit())]
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce')  # 将无法解析的日期转换为NaT并删除
    df.dropna(subset=['日期'], inplace=True) 
    return df


def main():
    filepath = '/Users/xuanming/Desktop/国泰君安/XGBoost预测规模因子收益方向/月宏观C.csv'
    df = load_data(filepath)
    X, y = preprocess_data(df)
    if y.isna().any():
        print("NaN detected in y before resampling:")
        print(y[y.isna()])
        return
    if X is None or y is None or X.size == 0 or y.size == 0:
        print("No data available after preprocessing.")
        return
    
    X_train, y_train, X_test, y_test, feature_names = prepare_data(X, y)
    if X_train.size == 0 or y_train.size == 0 or X_test.size == 0 or y_test.size == 0:
        print("One of the datasets is empty after preparing data.")
        return
    
    xgb_model, accuracy = train_model(X_train, y_train, X_test, y_test)
    xgb_model, accuracy01 = train_model(X_train, y_train, X_train, y_train)
    print(f'测试集准确率: {accuracy:.2f}')
    print(f'训练集准确率: {accuracy01:.2f}')
    print(xgb_model.get_booster().attributes())
    print(xgb_model.get_params()['n_estimators'])
    analyze_model(xgb_model, X_test, feature_names)
    xgb.plot_tree(xgb_model, num_trees=0)
    plt.show()

if __name__ == "__main__":
    main()