CREATE TABLE public.currency_rates_replica (
	id serial4 NOT NULL,
	currency_code varchar(10) NULL,
	currency_name text NULL,
	rate numeric NULL,
	exchange_date date NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	replicated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT currency_rates_replica_currency_code_exchange_date_key UNIQUE (currency_code, exchange_date),
	CONSTRAINT currency_rates_replica_pkey PRIMARY KEY (id)
);