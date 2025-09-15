CREATE TABLE IF NOT EXISTS exchange_rates (
    id SERIAL PRIMARY KEY,
    cc VARCHAR(10) NOT NULL,
    txt VARCHAR(100),
    rate NUMERIC,
    rate_per_100 NUMERIC,
    exchangedate DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    UNIQUE (cc, exchangedate)
);

CREATE TABLE IF NOT EXISTS telegram_users (
    chat_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    joined_at TIMESTAMP DEFAULT now()
);
