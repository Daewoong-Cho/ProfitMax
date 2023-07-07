use profitmax;

CREATE TABLE tbl_block_info (
    blockchain VARCHAR(10) NOT NULL,
    block_height INT NOT NULL,
    reward DECIMAL(18, 2) NOT NULL,
    difficulty DECIMAL(18, 0) NOT NULL,
    timestamp DATETIME NOT NULL,
    PRIMARY KEY (blockchain, block_height)
);

CREATE TABLE tbl_blockchain_info (
    blockchain VARCHAR(10) NOT NULL,
    subsidy DECIMAL(18, 2),
    difficulty DECIMAL(18, 0),
    last_updated DATETIME NOT NULL,
    PRIMARY KEY (blockchain)
);

CREATE TABLE tbl_blockdifficulty_history (
    blockchain VARCHAR(10) NOT NULL,
    difficulty DECIMAL(18, 0),
    timestamp DATETIME NOT NULL,
    PRIMARY KEY (blockchain, timestamp)
);


desc tbl_blockdifficulty_history;

CREATE TABLE tbl_crypto_price_tick (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	symbol VARCHAR(10) NOT NULL,
	price DECIMAL(18, 2) NOT NULL,
    currency_code VARCHAR(10) NOT NULL,
    timestamp DATETIME NOT NULL
);

CREATE TABLE tbl_energy_price_tick (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    location_id VARCHAR(10) NOT NULL,
	currency_code VARCHAR(10) NOT NULL,
	price DECIMAL(18, 5) NOT NULL,
    timestamp DATETIME NOT NULL
);

CREATE TABLE tbl_energy_price_current (
    location_id VARCHAR(10) NOT NULL,
	currency_code VARCHAR(10) NOT NULL,
	price DECIMAL(18, 5) NOT NULL,
    last_updated DATETIME NOT NULL,
	PRIMARY KEY (location_id, currency_code)
);

CREATE TABLE tbl_mining_cost_current (
    location_id VARCHAR(10) NOT NULL,
	cost_code VARCHAR(10) NOT NULL,
	currency_code VARCHAR(10) NOT NULL,
	price DECIMAL(18, 5) NOT NULL,
    last_updated DATETIME NOT NULL,
	PRIMARY KEY (location_id, cost_code, currency_code)
);
    
    
INSERT INTO tbl_mining_cost_current (location_id, cost_code, currency_code, price, last_updated) VALUES ('QLD1', 'CAPEX', 'AUD',  1, now());
INSERT INTO tbl_mining_cost_current (location_id, cost_code, currency_code, price, last_updated) VALUES ('QLD1', 'EMPLOYEE', 'AUD',  1, now());
INSERT INTO tbl_mining_cost_current (location_id, cost_code, currency_code, price, last_updated) VALUES ('QLD1', 'OFFICE', 'AUD',  1, now());

SET GLOBAL time_zone = '+10:00';