-- Simulated Trading Accounts Schema
-- Runs automatically on first MySQL container startup via /docker-entrypoint-initdb.d/

USE trading_arena;

CREATE TABLE IF NOT EXISTS sim_accounts (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    agent_name      VARCHAR(128) NOT NULL UNIQUE,
    starting_balance DOUBLE NOT NULL DEFAULT 150000,
    balance         DOUBLE NOT NULL DEFAULT 150000,
    start_of_day_balance DOUBLE NOT NULL DEFAULT 150000,
    realized_day_pnl DOUBLE NOT NULL DEFAULT 0,
    total_realized_pnl DOUBLE NOT NULL DEFAULT 0,
    total_profit    DOUBLE NOT NULL DEFAULT 0,
    total_loss      DOUBLE NOT NULL DEFAULT 0,
    highest_balance DOUBLE NOT NULL DEFAULT 150000,
    highest_unrealized_balance DOUBLE NOT NULL DEFAULT 150000,
    highest_realized_balance DOUBLE NOT NULL DEFAULT 150000,
    drawdown_limit  DOUBLE NOT NULL DEFAULT 4500,
    mll_floor       DOUBLE NOT NULL DEFAULT 145500,
    total_trades    INT NOT NULL DEFAULT 0,
    daily_trades    INT NOT NULL DEFAULT 0,
    winning_trades  INT NOT NULL DEFAULT 0,
    losing_trades   INT NOT NULL DEFAULT 0,
    can_trade       TINYINT(1) NOT NULL DEFAULT 1,
    blown           TINYINT(1) NOT NULL DEFAULT 0,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sim_positions (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id  BIGINT NOT NULL,
    symbol      VARCHAR(64) NOT NULL,
    quantity    INT NOT NULL,
    avg_price   DOUBLE NOT NULL,
    tick_size   DOUBLE NOT NULL,
    tick_value  DOUBLE NOT NULL,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_account_symbol (account_id, symbol),
    FOREIGN KEY (account_id) REFERENCES sim_accounts(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sim_trades (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id    BIGINT NOT NULL,
    symbol        VARCHAR(64) NOT NULL,
    side          VARCHAR(8) NOT NULL,
    quantity      INT NOT NULL,
    entry_price   DOUBLE NOT NULL,
    exit_price    DOUBLE NOT NULL,
    realized_pnl  DOUBLE NOT NULL,
    is_win        TINYINT(1) NOT NULL,
    tick_size     DOUBLE NOT NULL,
    tick_value    DOUBLE NOT NULL,
    opened_at     DATETIME,
    closed_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    duration_secs INT NOT NULL DEFAULT 0,
    FOREIGN KEY (account_id) REFERENCES sim_accounts(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sim_orders (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id    BIGINT NOT NULL,
    symbol        VARCHAR(64) NOT NULL,
    side          VARCHAR(8) NOT NULL,
    quantity      INT NOT NULL,
    fill_price    DOUBLE NOT NULL DEFAULT 0,
    status        VARCHAR(16) NOT NULL DEFAULT 'FILLED',
    reject_reason VARCHAR(256),
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES sim_accounts(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sim_daily_snapshots (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id      BIGINT NOT NULL,
    trade_date      DATE NOT NULL,
    balance         DOUBLE NOT NULL,
    equity          DOUBLE NOT NULL,
    realized_pnl    DOUBLE NOT NULL DEFAULT 0,
    cumulative_pnl  DOUBLE NOT NULL DEFAULT 0,
    mll_floor       DOUBLE NOT NULL,
    trade_count     INT NOT NULL DEFAULT 0,
    win_count       INT NOT NULL DEFAULT 0,
    UNIQUE KEY uq_account_date (account_id, trade_date),
    FOREIGN KEY (account_id) REFERENCES sim_accounts(id)
) ENGINE=InnoDB;
