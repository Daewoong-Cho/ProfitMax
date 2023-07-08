import json
import pymysql
import pandas as pd
import datetime
import argparse
import time
import numpy as np
from sqlalchemy import create_engine

from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from keras.models import Sequential
from keras.layers import LSTM, Dense

#python3 sh_predict_energy_price.py --type Energy --symbol BTC --location QLD1 --model Linear --interval 10
#python3 sh_predict_energy_price.py --type Energy --symbol BTC --location QLD1 --model RandomForest --interval 10
#python3 sh_predict_energy_price.py --type Energy --symbol BTC --location QLD1 --model LSTM --interval 10
#python3 sh_predict_energy_price.py --type Crypto --symbol BTC --location QLD1 --model Linear --interval 10
#python3 sh_predict_energy_price.py --type Crypto --symbol BTC --location QLD1 --model RandomForest --interval 10
#python3 sh_predict_energy_price.py --type Crypto --symbol BTC --location QLD1 --model LSTM --interval 10

# JSON 파일을 읽어옵니다.
with open('dbconfig.json') as json_file:
    config = json.load(json_file)

# JSON 파일에서 필요한 정보를 추출합니다.
host = config['host']
port = config['port']
user = config['user']
password = config['password']
database = config['database']

# MySQL 데이터베이스 연결 설정
# SQLAlchemy를 사용하여 데이터베이스 연결을 설정합니다.
# 여기에서는 MySQL 데이터베이스를 사용하는 예시입니다.
database_uri = 'mysql+mysqlconnector://'+user+':' + password + '@' + host + ':' + str(port) + '/' + database 
engine = create_engine(database_uri)

# ArgumentParser 객체를 생성합니다.
parser = argparse.ArgumentParser(description='Price Prediction')

# 인수를 추가합니다.
parser.add_argument('--type', type=str, help='Data Type(Energy, Crypto)')
parser.add_argument('--symbol', type=str, help='Blockchain Symbol(default: BTC)')
parser.add_argument('--location', type=str, help='Location ID(default: QLD1)')
parser.add_argument('--model', type=str, help='Prediction Model(default: Linear Regression)')
parser.add_argument('--interval', type=int, help='Interval (default: 10sec)')

# 명령행 인수를 파싱합니다.
args = parser.parse_args()

type = args.type
location = args.location 
symbol = args.symbol
model = args.model
interval = args.interval

if args.location is None: location = "QLD1"
if args.symbol is None: symbol = "BTC"
if args.model is None: model = "Linear"
if args.interval is None: interval = 10

def repeated_task(interval):
    while True:
        # 데이터 가져오기
        if args.type == "Energy":
            query = "SELECT timestamp, price FROM tbl_energy_price_tick where location_id = '" + location + "'"
        else:
            query = "SELECT timestamp, price FROM tbl_crypto_price_tick WHERE symbol = '" + symbol + "'"


        data = pd.read_sql_query(query, engine)

        # 데이터 전처리
        data['timestamp'] = pd.to_datetime(data['timestamp'])  # timestamp 열을 datetime 형식으로 변환
        data['price'] = pd.to_numeric(data['price'], errors='coerce')  # price 열을 float64 형식으로 변환

        # 특징과 타겟 데이터 분리
        X = data['timestamp'].apply(lambda x: x.timestamp()).values.reshape(-1, 1)  # 특징 데이터
        y = data['price'].values  # 타겟 데이터


        # 훈련 및 테스트 데이터 분할
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # 선형 회귀 모델 훈련
        if args.model == "RandomForest":
            model = LinearRegression()
        elif args.model == "LSTM":
            # LSTM 모델 구축
            model = Sequential()
            #model.add(LSTM(units=12, activation='relu', input_shape=(X.shape[1], 1))) #unit이 커질수록 cost가 커짐
            model.add(LSTM(units=12, activation='sigmoid', input_shape=(X.shape[1], 1)))
            model.add(Dense(units=1))
            model.compile(optimizer='adam', loss='mean_squared_error')
            # LSTM 모델 훈련
            model.fit(X_train, y_train, epochs=10, batch_size=32)

        else: 
            model = RandomForestRegressor(n_estimators=100, random_state=42)

        model.fit(X_train, y_train)

        # 예측 수행
        if args.model == "LSTM":
            # LSTM 모델 입력 데이터 형식 변환
            X_test_lstm = X_test.reshape((X_test.shape[0], X_test.shape[1], 1))
            y_pred = model.predict(X_test_lstm)
        else:
            y_pred = model.predict(X_test)

        # 예측 결과 평가
        mse = mean_squared_error(y_test, y_pred)
        #print('Mean Squared Error:', mse)

        # 원하는 시간에 대한 가격 예측
        future_timestamp = datetime.datetime.now() + datetime.timedelta(minutes=5)
        future_timestamp = future_timestamp.timestamp()  # POSIX 시간(에포크 시간)으로 변환
        if args.model == "LSTM":
            # LSTM 모델 입력 데이터 형식 변환
            future_timestamp_lstm = np.array([[future_timestamp]])
            future_timestamp_lstm = future_timestamp_lstm.reshape((future_timestamp_lstm.shape[0], future_timestamp_lstm.shape[1], 1))
            future_price = model.predict(future_timestamp_lstm)
        else:
            future_price = model.predict([[future_timestamp]])

        print('Predicted Price ', future_price, ' at Future Time:', datetime.datetime.fromtimestamp(future_timestamp))
        
        # X초 대기
        time.sleep(interval)
# 정해진 X초마다 반복되도록 작업 실행
repeated_task(interval)