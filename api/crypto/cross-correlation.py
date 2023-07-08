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
from scipy.signal import correlate

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

# 크로스-상관관계 계산
cross_corr = correlate(merged_data['energy_price'], merged_data['bitcoin_price'], mode='same')

plt.plot(merged_data['timestamp'], cross_corr)
plt.xlabel('Time Lag')
plt.ylabel('Cross Correlation')
plt.show()