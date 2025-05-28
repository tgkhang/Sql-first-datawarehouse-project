-- check prd key
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_prd_key not in (
select prd_key from 
silver.crm_prd_info)
-- result no transformation for prd key

-- check the same for cust id,
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_cust_id not in (
select cst_id from 
silver.crm_cust_info)

-- result no transformation for cust_id

-- check date order, ship, due\
-- check for invalid date
select NULLIF(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 or len(sls_order_dt)!= 8
--OR sls_order_dt > 20300101
--or sls_order_dt < 20000101

select NULLIF(sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 or len(sls_ship_dt)!= 8
--OR sls_ship_dt > 20300101
--or sls_ship_dt < 20000101


select NULLIF(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 or len(sls_due_dt)!= 8
--OR sls_due_dt > 20300101
--or sls_due_dt < 20000101

--date should be in order
select *
from bronze.crm_sales_details
where sls_order_dt >sls_ship_dt or sls_order_dt > sls_due_dt

-- check data consistency betweeen sales quantity and price
--sale= quantity * price
-- value must not be null zero or negagtive
select distinct --sls_sales, sls_quantity, sls_price
sls_sales as old_sls_sales,
	sls_quantity,
	sls_price as old_sls_price,
	case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity *abs(sls_price)
		then sls_quantity * abs(sls_price)
		else sls_sales
	end as sls_sales,
	case when  sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity,0)
		else sls_price
	end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales, sls_quantity, sls_price



-- FINAL transition
insert into silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales, 
	sls_quantity, 
	sls_price
)
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	Case when sls_order_dt = 0 or len(sls_order_dt)!= 8 then NULL
		else cast(cast(sls_order_dt as varchar)as date)
	end as sls_order_dt,
	Case when sls_ship_dt = 0 or len(sls_ship_dt)!= 8 then NULL
		else cast(cast(sls_ship_dt as varchar)as date)
	end as sls_ship_dt,
	Case when sls_due_dt = 0 or len(sls_due_dt)!= 8 then NULL
		else cast(cast(sls_due_dt as varchar)as date)
	end as sls_due_dt,
	--sls_sales as old_sls_sales,
	sls_quantity,
	--sls_price as old_sls_price,
	case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity *abs(sls_price)
		then sls_quantity * abs(sls_price)
		else sls_sales
	end as sls_sales,
	case when  sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity,0)
		else sls_price
	end as sls_price
from bronze.crm_sales_details
