select 
	ci.cst_id, 
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca 
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key= la.cid

--check duplicate
select cst_id,count(*) 
from (
	select 
		ci.cst_id, 
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	from silver.crm_cust_info as ci
	left join silver.erp_cust_az12 ca 
	on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
	on ci.cst_key= la.cid
) t group by cst_id
having count(*) >1


--validate gender
select distinct
	ci.cst_gndr,
	ca.gen
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca 
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key= la.cid
order by 1,2


--fix using data integration
select distinct
	ci.cst_gndr,
	ca.gen,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- use data from master crm
		else coalesce(ca.gen, 'n/a')
	end as new_gen
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca 
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key= la.cid
order by 1,2


-- enhance main query
select 
	ci.cst_id, 
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- use data from master crm
		else coalesce(ca.gen, 'n/a')
	end as new_gen,
	ci.cst_create_date,
	ca.bdate,
	la.cntry
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca 
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key= la.cid


-- rename, using nice friendly name, add key
select 
	ROW_NUMBER() OVER (Order by cst_id)as customer_key,
	ci.cst_id as customer_id, 
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- use data from master crm
		else coalesce(ca.gen, 'n/a')
	end as gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca 
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key= la.cid


-- create object
create view gold.dim_customers as
select 
	ROW_NUMBER() OVER (Order by cst_id)as customer_key,
	ci.cst_id as customer_id, 
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- use data from master crm
		else coalesce(ca.gen, 'n/a')
	end as gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca 
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key= la.cid