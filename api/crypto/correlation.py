import json
import pandas as pd
import argparse
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
from sqlalchemy import create_engine

from scipy.stats import pearsonr
from scipy.stats import spearmanr
from scipy.stats import kendalltau
from scipy.stats import pointbiserialr

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
parser = argparse.ArgumentParser(description='Correlation Analysis')

# 인수를 추가합니다.
parser.add_argument('--symbol', type=str, help='Blockchain Symbol(default: BTC)')
parser.add_argument('--location', type=str, help='Location ID(default: QLD1)')

# 명령행 인수를 파싱합니다.
args = parser.parse_args()

location = args.location 
symbol = args.symbol

if args.location is None: location = "QLD1"
if args.symbol is None: symbol = "BTC"

# 에너지 가격 데이터와 Bitcoin 가격 데이터를 로드합니다.
query = "SELECT timestamp, price as energy_price FROM tbl_energy_price_tick where location_id = '" + location + "'"
energy_data =  pd.read_sql_query(query, engine)
query = "SELECT timestamp, price as bitcoin_price FROM tbl_crypto_price_tick WHERE symbol = '" + symbol + "'"

bitcoin_data= pd.read_sql_query(query, engine)

# 데이터 프레임을 날짜/시간 형식으로 변환
energy_data['timestamp'] = pd.to_datetime(energy_data['timestamp'])
bitcoin_data['timestamp'] = pd.to_datetime(bitcoin_data['timestamp'])

# 날짜/시간을 기준으로 데이터 프레임 병합
merged_data = pd.merge(energy_data, bitcoin_data, on='timestamp')

# 상관관계 분석
correlation, p_value = pearsonr(merged_data['energy_price'], merged_data['bitcoin_price'])
print('Pearson Correlation:', correlation, 'p-value:', p_value)

correlation, p_value = spearmanr(merged_data['energy_price'], merged_data['bitcoin_price'])
print('Spearmans Rank Correlation:', correlation, 'p-value:', p_value)

correlation, p_value = kendalltau(merged_data['energy_price'], merged_data['bitcoin_price'])
print('Kendalls Rank Correlation:', correlation, 'p-value:', p_value)

correlation, p_value = pointbiserialr(merged_data['energy_price'], merged_data['bitcoin_price'])
print('Point-Biserial Correlation:', correlation, 'p-value:', p_value)

fig, ax1 = plt.subplots()

# 데이터 프레임을 날짜 형식으로 변환
merged_data['timestamp'] = pd.to_datetime(merged_data['timestamp'])

# 일자별 그룹핑 및 평균 계산
daily_data = merged_data.groupby(merged_data['timestamp'].dt.date).mean()

# 첫 번째 축 (에너지 가격)
ax1.plot(daily_data.index, daily_data['energy_price'], label='Energy Prices', color='blue')
ax1.set_xlabel('Date')
ax1.set_ylabel('Energy Price')
ax1.tick_params(axis='y')

# x 축에 날짜 형식 적용
ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

# 두 번째 축 (비트코인 가격)
ax2 = ax1.twinx()
ax2.plot(daily_data.index, daily_data['bitcoin_price'], label='Bitcoin Prices', color='orange')
ax2.set_ylabel('Bitcoin Price')
ax2.tick_params(axis='y')

# 범례 표시
lines = ax1.get_lines() + ax2.get_lines()
labels = [line.get_label() for line in lines]
ax1.legend(lines, labels, loc='upper left')

plt.show()
