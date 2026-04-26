CREATE dim_payment_details(
  details_id INT SERIAL PRIMARY KEY,
  currency TEXT NOT NULL,
  status TEXT NOT NULL,
  order_id TEXT NOT NULL,
  product TEXT NOT NULL,
  channel TEXT NOT NULL,
  region TEXT NOT NULL,
  customer_type TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);


CREATE facts_payments(
  payment_id INT SERIAL PRIMARY KEY, 
  details_id INT NOT NULL REFERENCES dim_payment_details(details_id),
  amount INT NOT NULL,
  amount_received INT NOT NULL,
  created INT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE index indx_facts_payments ON facts_payments (payment_id);


