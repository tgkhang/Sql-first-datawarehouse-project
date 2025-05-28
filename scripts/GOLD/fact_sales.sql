select 
sd.sls_ord_num,
sd.sls_prd_key,
sd.sls_cust_id,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
from silver.crm_sales_details sd

--build fact table
-- data look up for key
select 
sd.sls_ord_num as order_number,
pr.product_key ,
--sd.sls_prd_key,
--sd.sls_cust_id,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_ammount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id

---
create view gold.fact_sales as 
select 
sd.sls_ord_num as order_number,
pr.product_key ,
--sd.sls_prd_key,
--sd.sls_cust_id,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_ammount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
