SELECT *
FROM C01_l01_ecommerce_retail_data_table
LIMIT 20;



SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date,
    SUM(CASE WHEN customer_segment IS NULL THEN 1 ELSE 0 END) AS null_customer_segment,
    SUM(CASE WHEN cost IS NULL THEN 1 ELSE 0 END) AS null_cost,
    SUM(CASE WHEN is_return IS NULL THEN 1 ELSE 0 END) AS null_is_return,
    SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END) AS null_payment_method,
    SUM(CASE WHEN order_amount_old IS NULL THEN 1 ELSE 0 END) AS null_order_amount_old,
    SUM(CASE WHEN hour_of_day IS NULL THEN 1 ELSE 0 END) AS null_hour_of_day
FROM C01_l01_ecommerce_retail_data_table;



--Count customer_segment
SELECT customer_segment, COUNT(*) AS nbr_segments
FROM C01_l01_ecommerce_retail_data_table
GROUP BY customer_segment
ORDER BY nbr_segments DESC;



--Count payment_method 
SELECT payment_method, COUNT(*) AS nbr_methods
FROM C01_l01_ecommerce_retail_data_table
GROUP BY payment_method
ORDER BY nbr_methods DESC;


--Creating a silver_parsed view 
CREATE TEMP VIEW silver_parsed AS
SELECT 
     row_id,
     COALESCE(
       try_strptime(replace(date,'.','-'), '%Y-%m-%d'),
       try_strptime(replace(date,'.','-'), '%d-%m-%Y')
     ) AS parsed_dt,
     lower(trim(customer_segment)) AS customer_segment_raw,
     try_cast(order_amount_old AS DOUBLE) AS order_amount_old,
     try_cast(cost AS DOUBLE) AS cost,
     try_cast(is_return AS INTEGER) AS is_return,
     payment_method,
     try_cast(hour_of_day AS INTEGER) AS hour_of_day
FROM C01_l01_ecommerce_retail_data_table;

--Checks
SELECT 
   COUNT(*) AS total_rows,
   SUM(CASE WHEN parsed_dt IS NULL THEN 1 ELSE 0 END) AS date_parse_failures
FROM silver_parsed
     

--Applying Changes to dates and finding any failures 
CREATE TEMP VIEW silver_parsed AS
SELECT 
     row_id,
     COALESCE(
       try_strptime(replace(date,'.','-'), '%Y-%m-%d'),
       try_strptime(replace(date,'.','-'), '%d-%m-%Y')
     ) AS parsed_dt,
     lower(trim(customer_segment)) AS customer_segment_raw,
     try_cast(order_amount_old AS DOUBLE) AS order_amount_old,
     try_cast(cost AS DOUBLE) AS cost,
     try_cast(is_return AS INTEGER) AS is_return,
     payment_method,
     try_cast(hour_of_day AS INTEGER) AS hour_of_day
FROM C01_l01_ecommerce_retail_data_table;
--Checks
SELECT 
   COUNT(*) AS total_rows,
   SUM(CASE WHEN parsed_dt IS NULL THEN 1 ELSE 0 END) AS date_parse_failures
FROM silver_parsed
     


--Checking for further changes 
SELECT *
FROM silver_parsed
LIMIT 10;


--Changing and standardising few typos and date format
CREATE TEMP VIEW silver_normalised AS
SELECT
   row_id,
   parsed_dt,
   /* Standard DD-MM-YYYY string */
   strftime(parsed_dt::DATE, '%d-%m-%Y') AS date,
   /* Map common typos to canonical tiers */
   CASE
      WHEN customer_segment_raw IS NULL THEN NULL
      WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN  ('standrad') THEN 'standard'
      WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN  ('premuim') THEN 'premium'
      WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN  ('platnum') THEN 'platinum'
      ELSE customer_segment_raw
    END AS customer_segment_raw,
    order_amount_old,
    cost,
    CASE WHEN is_return IN (0,1) THEN is_return ELSE NULL END AS is_return,
    payment_method,
    hour_of_day
FROM silver_parsed;
--  LOOK SEE
SELECT *
FROM silver_normalised
LIMIT 10;



--Checks 
SELECT customer_segment_raw, COUNT(*) AS nbr_segments
FROM silver_normalised
GROUP BY customer_segment_raw
ORDER BY nbr_segments DESC;



-- Pre-filter issue counts (diagnostics)
SELECT
  SUM(CASE WHEN parsed_dt IS NULL THEN 1 ELSE 0 END) AS bad_date,
  SUM(CASE WHEN order_amount_old IS NULL OR order_amount_old < 5.0 THEN 1 ELSE 0 END) AS bad_amount,
  SUM(CASE WHEN cost IS NULL OR cost <=0 THEN 1 ELSE 0 END) AS bad_cost,
  SUM(CASE WHEN is_return NOT IN (0,1) OR is_return IS NULL THEN 1 ELSE 0 END) AS bad_return_flag,
  SUM(CASE WHEN hour_of_day IS NULL OR hour_of_day NOT BETWEEN 0 AND 23 THEN 1 ELSE 0 END) AS bad_hour
FROM silver_normalised;



CREATE TEMP VIEW silver_filtered AS 
SELECT *
FROM silver_normalised 
WHERE
 parsed_dt IS NOT NULL
 AND order_amount_old IS NOT NULL AND order_amount_old >= 5.0
 AND cost IS NOT NULL AND cost > 0
 AND is_return IS NOT NULL
 AND hour_of_day BETWEEN 0 AND 23;
--Post-Filter retained count
SELECT COUNT(*) AS kept_after_filter
FROM silver_filtered




-- Distict rowss (by full business key)
SELECT COUNT(*) AS distinct_rows
FROM (
     SELECT DISTINCT
     row_id, date, customer_segment_raw, order_amount_old, cost, is_return, payment_method, hour_of_day
     FROM silver_filtered
);



--Remove all duplicates (keep one of each busnmess two)
CREATE OR REPLACE TEMP VIEW clean_table AS
SELECT DISTINCT
         row_id, date, customer_segment_raw, order_amount_old, cost, is_return, payment_method, hour_of_day
FROM silver_filtered;


--QUICK LOOK SEE
SELECT *
FROM clean_table
LIMIT 10;



--KPI_1 :  Average Order Value (AOV)
CREATE TEMP VIEW kpi_1 AS
SELECT 
   'kpi_1' AS kpi_name,
   CAST(ROUND(AVG(order_amount_old),2)AS VARCHAR) AS kpi_value,
   CAST (NULL AS VARCHAR) AS kpi_key
   FROM clean_table;
SELECT *
   FROM kpi_1 


--KPI_2 : Overall Gross Margin % - Total profit (revenue minus cost) as a percentage of total revenue.
CREATE TEMP VIEW kpi_2 AS 
SELECT 
    'kpi_2' AS kpi_name,
    CAST(ROUND( (SUM(order_amount_old - cost) / SUM(order_amount_old)), 6) AS VARCHAR) AS kpi_value,
    CAST (NULL AS VARCHAR) AS kpi_key  
FROM clean_table;
SELECT *
FROM kpi_2


--KPI_3 : Return Rate
CREATE TEMP VIEW kpi_3 AS 
SELECT 
    'kpi_3' AS kpi_name,
    CAST(ROUND( SUM(is_return) / COUNT(*), 6) AS VARCHAR) AS kpi_value,
    CAST (NULL AS VARCHAR) AS kpi_key  
FROM clean_table;

SELECT *
FROM kpi_3


--KPI_4 : Median Order Amount
CREATE TEMP VIEW kpi_4 AS 
SELECT 
    'kpi_4' AS kpi_name,
    CAST(ROUND(median(order_amount_old), 2) AS VARCHAR) AS kpi_value,
    CAST (NULL AS VARCHAR) AS kpi_key  
FROM clean_table;

SELECT *
FROM kpi_4


--KPI_5 : Return Rate by Payment Method
CREATE TEMP VIEW kpi_5 AS 
SELECT 
    'kpi_5' AS kpi_name,
    CAST(ROUND(SUM(is_return) / COUNT(*), 6) AS VARCHAR) AS kpi_value,
    CAST (payment_method AS VARCHAR) AS kpi_key  
FROM clean_table
GROUP BY payment_method;

SELECT *
FROM kpi_5


--KPI_6 : High-Value Customer GMV Share
CREATE TEMP VIEW kpi_6 AS
WITH gmv AS (
SELECT 
SUM(order_amount_old) AS total_gmv,
SUM(CASE WHEN customer_segment_raw IN ('premium','platinum') THEN order_amount_old ELSE 0 END) AS hv_gmv
FROM clean_table
) 
SELECT 
    'kpi_6' AS kpi_name,
    CAST(ROUND(hv_gmv / total_gmv, 6) AS VARCHAR) AS kpi_value,
    CAST (NULL AS VARCHAR) AS kpi_key  
FROM gmv;

SELECT *
FROM kpi_6


--KPI_7 : Below-Target Margin Rate
CREATE TEMP VIEW kpi_7 AS
WITH base AS (
SELECT 
customer_segment_raw,
(order_amount_old - cost) / order_amount_old  AS gross_margin
FROM clean_table 
),
eligible AS (
SELECT 
   customer_segment_raw,
   gross_margin,
   CASE 
      WHEN customer_segment_raw = 'standard' THEN 0.40
      WHEN customer_segment_raw = 'premium' THEN 0.30
      WHEN customer_segment_raw = 'platinum' THEN 0.25
   END AS floor_margin 
  FROM base
  WHERE customer_segment_raw IN ('standard', 'premium', 'platinum')
)
SELECT 
   'kpi_7' AS kpi_name,
   CAST(
     ROUND(
      1.0 * SUM(
      CASE 
          WHEN customer_segment_raw = 'platinum' AND gross_margin <= floor_margin THEN 1
          WHEN customer_segment_raw IN ('standard', 'premium') AND gross_margin < floor_margin THEN 1
            ELSE 0
      END
      ) / COUNT(*), 6
    ) AS VARCHAR
 ) AS kpi_value,
   CAST (NULL AS VARCHAR) AS kpi_key
FROM eligible;

SELECT *
FROM kpi_7


--KPI_8 : Top-GMV Month in 2024
CREATE TEMP VIEW kpi_8 AS 
WITH month_gmv AS (
   SELECT 
    strftime(strptime(date, '%d-%m-%Y'), '%Y-%m') AS month_key,
    SUM(order_amount_old) AS gmv
  FROM clean_table
  GROUP BY month_key
)
SELECT 
   'kpi_8' AS kpi_name,
  CAST(month_key AS VARCHAR) AS kpi_value,
  CAST(NULL AS VARCHAR) AS kpi_key
FROM month_gmv
ORDER BY gmv DESC, month_key DESC
LIMIT 1;

SELECT * FROM kpi_8;



--- KPI_9 : Latest MoM GMV growth 
CREATE TEMP VIEW kpi_9 AS
WITH month_gmv AS (
    SELECT 
       strftime(strptime(date, '%d-%m-%Y'), '%Y-%m') AS month_key,
       SUM(order_amount_old) AS gmv
    FROM clean_table
    GROUP BY month_key
),
with_lag AS (
  SELECT 
    month_key,
    gmv,
    LAG(gmv) OVER (ORDER BY month_key) AS prev_gmv
  FROM month_gmv
),
latest AS (
  SELECT* FROM with_lag ORDER BY month_key DESC LIMIT 1
)
SELECT 
   'kpi_9' AS kpi_name,
   CAST(ROUND( (gmv - prev_gmv) / prev_gmv, 6)AS VARCHAR) AS kpi_value,
   CAST(NULL AS VARCHAR) AS kpi_key
FROM latest;

SELECT * 
FROM kpi_9;
  


--KPI_10 : Max Month-to-Month Payment-Method Share Shift (pp)
CREATE TEMP VIEW kpi_10 AS
WITH  with_month AS (
SELECT 
  strftime(strptime(date, '%d-%m-%Y'), '%Y-%m') AS month_key,
  payment_method
FROM clean_table
),
counts AS(
  SELECT month_key, payment_method, COUNT(*) AS n 
  FROM with_month
  GROUP BY month_key, payment_method
  ),
totals AS (
  SELECT month_key, SUM(n) AS total
  FROM counts
  GROUP BY month_key
),
shares AS (
  SELECT c.month_key, c.payment_method, 1.0 * c.n / t.total AS share
  FROM counts c
  JOIN totals t USING (month_key)
),
diffs AS (
  SELECT 
    payment_method,
    month_key,
    ABS(share - LAG(share) OVER (PARTITION BY payment_method ORDER BY month_key)) AS diff
    FROM shares
)
SELECT 
  'kpi_10' AS kpi_name,
  CAST(ROUND(MAX(diff), 6) AS VARCHAR ) AS kpi_value,
  CAST(NULL AS VARCHAR) AS kpi_key
FROM diffs
WHERE diff IS NOT NULL;
SELECT * FROM kpi_10;








