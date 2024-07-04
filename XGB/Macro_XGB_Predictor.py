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
    # 删除序号列
    if '序号' in df.columns:
        df.drop(['序号'], axis=1, inplace=True)
    # 确保日期列是日期格式
    df['日期'] = pd.to_datetime(df['日期'], format='%Y-%m-%d', errors='coerce')
    df.set_index('日期', inplace=True)

    # 处理所有列，尝试转换为浮点数，移除逗号
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')

    # 严格清除含有任何NaN的行
    df.dropna(inplace=True, how='any')  # 确保每一行都没有NaN值

    # 聚合上综指到月度涨跌（1，0）
    if '上证综合指数' in df.columns:
        monthly_index = df['上证综合指数'].resample('M').last()
        monthly_diff = monthly_index.diff().fillna(0)
        monthly_up_down = monthly_diff.apply(lambda x: 1 if x > 0 else 0)
        df = df.resample('M').last()
        df['指数月度涨跌'] = monthly_up_down.values

    # 删除没用的列
    X = df.drop(['指数月度涨跌', '上证综合指数'], axis=1) if '上证综合指数' in df.columns else df.drop(['指数月度涨跌'], axis=1)
    y = df['指数月度涨跌']

    return X, y

def prepare_data(X, y):
    if X.empty or y.empty:
        raise ValueError("Input data X or y is empty.")
    feature_names = X.columns.tolist()  # 保存原始特征名
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train_scaled = pd.DataFrame(scaler.fit_transform(X_train), columns=feature_names)
    X_test_scaled = pd.DataFrame(scaler.transform(X_test), columns=feature_names)
    ros = RandomOverSampler(random_state=42)
    X_resampled, y_resampled = ros.fit_resample(X_train_scaled, y_train)
    return X_resampled, y_resampled, X_test_scaled, y_test, feature_names

def train_model(X_train, y_train, X_test, y_test):
    xgb_model = xgb.XGBClassifier(eval_metric='logloss', use_label_encoder=False)
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
    if X is None or y is None or X.size == 0 or y.size == 0:
        print("No data available after preprocessing.")
        return
    
    X_train, y_train, X_test, y_test, feature_names = prepare_data(X, y)
    if X_train.size == 0 or y_train.size == 0 or X_test.size == 0 or y_test.size == 0:
        print("One of the datasets is empty after preparing data.")
        return
    
    xgb_model, accuracy = train_model(X_train, y_train, X_test, y_test)
    print(f'测试集准确率: {accuracy:.2f}')
    analyze_model(xgb_model, X_test, feature_names)

if __name__ == "__main__":
    main()


