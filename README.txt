sh /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties
sh /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties
sh /usr/local/kafka/bin/kafka-topics.sh --create --topic public.cryptoprice --bootstrap-server 10.0.2.15:9092
sh /usr/local/kafka/bin/kafka-topics.sh --list --bootstrap-server 10.0.2.15:9092

Process Management

sudo apt-get install supervisor

/etc/supervisor/conf.d/profitmax.conf


sudo supervisorctl reread
sudo supervisorctl update


sudo supervisorctl start myapp
sudo supervisorctl stop myapp
sudo supervisorctl restart myapp



sudo supervisorctl start p_block_info_api
sudo supervisorctl start p_block_info_db
sudo supervisorctl start p_crypto_price_api
sudo supervisorctl start p_crypto_price_db
sudo supervisorctl start p_mining_decision_maker
sudo supervisorctl start p_energy_price_api
sudo supervisorctl start p_energy_price_db
sudo supervisorctl start p_mining_incentive_calculator
sudo supervisorctl start p_energy_cost_calculator
sudo supervisorctl start p_mining_cost_calculator

sudo supervisorctl stop p_block_info_api
sudo supervisorctl stop p_block_info_db
sudo supervisorctl stop p_crypto_price_api
sudo supervisorctl stop p_crypto_price_db
sudo supervisorctl stop p_mining_decision_maker
sudo supervisorctl stop p_energy_price_api
sudo supervisorctl stop p_energy_price_db
sudo supervisorctl stop p_mining_incentive_calculator
sudo supervisorctl stop p_energy_cost_calculator
sudo supervisorctl stop p_mining_cost_calculator

sudo supervisorctl restart p_block_info_api
sudo supervisorctl restart p_block_info_db
sudo supervisorctl restart p_crypto_price_api
sudo supervisorctl restart p_crypto_price_db
sudo supervisorctl restart p_mining_decision_maker
sudo supervisorctl restart p_energy_price_api
sudo supervisorctl restart p_energy_price_db
sudo supervisorctl restart p_mining_incentive_calculator
sudo supervisorctl restart p_energy_cost_calculator
sudo supervisorctl restart p_mining_cost_calculator

go build p_block_info_api.go
go build p_crypto_price_api.go
go build p_mining_decision_maker.go
go build p_crypto_price_db.go
go build p_energy_price_api.go
go build p_block_info_db.go
go build p_energy_price_db
go build p_mining_incentive_calculator.go
go build p_energy_cost_calculator.go
go build p_mining_cost_calculator.go
mysql -u profitmax -p


#React 실행하기
1. create-react-app 설치

npm install -g create-react-app
 

2. react project 생성 -> 원하는 이름으로 생성하면 된다.

create-react-app dashboard
 

3. 생성한 폴더로 이동 후 app을 실행시키면 아래와 같은 페이지가 생성된 것을 확인할 수 있다.

cd test_app
npm start

4. App.js 파일에 들어가 수정하고 save를 하면 자동으로 컴파일이 시작되어, 페이지가 자동으로 reload 되는 것도 확인할 수 있다.

SET GLOBAL time_zone = '+10:00';

python3 sh_predict_energy_price.py --type Energy --symbol BTC --location QLD1 --model Linear --interval 10
python3 sh_predict_energy_price.py --type Energy --symbol BTC --location QLD1 --model RandomForest --interval 10
python3 sh_predict_energy_price.py --type Crypto --symbol BTC --location QLD1 --model Linear --interval 10
python3 sh_predict_energy_price.py --type Crypto --symbol BTC --location QLD1 --model RandomForest --interval 10
