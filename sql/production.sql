CREATE dim_product_details(
  details_id SERIAL PRIMARY KEY ,
  product_name TEXT NOT NULL ,
  category TEXT NOT NULL,
  region TEXT NOT NULL,
categorycrested_on TIMESTAMPS NOW()
);


CREATE dim_dates(
  date_id DATE PRIMARY KEY ,
  year INT,
  month INT,
  day INT
);


CREATE facts_production(
  product_id SERIAL PRIMARY KEY,
  
  details_id  INT NOT NULL REFERENCES
  dim_product_details(details_id),

  date_id DATE NOT NULL  REFERENCES
  dim_dates(date_id),

  units_produced INT NOT NULL,
  defective_units INT NOT NULL,
  revenue INT NOT NULL ,
  cost INT NOT NULL,
  analyst_score INT NOT NULL,
  crested_on TIMESTAMPS NOW()
);