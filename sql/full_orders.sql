--analytic schemas 

CREATE  dim_order_prodcuts(
  products_id INT SERIAL PRIMARY KEY,
  product  TEXT NOT NULL,
  region TEXT,
  created_on  TIMESTAMP DEFAULT NOW()
);

CREATE dim_order_date(
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT
);


CREATE facts_full_orders(
  order_id iNT SERIAL PRIMARY KEY ,
  product_id INT NOT NULL REFERENCES dim_order_products(product_id),
  date_id  DATE NOT NULL REFERENCE dim_order_date(date_id)
  quantity INT NOT NULL,
  unit_price INT NOT NULL,
  cost_per_unit INT NOT NULL 
  created_at TIMESTAMP DEFAULT NOW()
);


CREATE index indx_full_orders ON facts_full_orders(order_id);
