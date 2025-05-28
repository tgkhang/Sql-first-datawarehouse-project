select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
--pn.prd_end_dt
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null -- filter out all historical data


-- check product key is unique
select prd_key, count(*) from
(
select 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	--pn.prd_end_dt
	pc.cat,
	pc.subcat,
	pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null -- filter out all historical data

) t group by prd_key
having count(*) >1
--res: OK NO Duplicate

-- Group collum for readability and change to friendly name
select 
	ROW_NUMBER() OVER (Order by pn.prd_start_dt, pn.prd_key)as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.cat_id as product_name,
	pc.cat as category_id,	
	pc.subcat as category,
	pc.maintenance as subcategory,
	pn.prd_nm ,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null -- filter out all historical data

--BUILD view

create view gold.dim_products as
select 
	ROW_NUMBER() OVER (Order by pn.prd_start_dt, pn.prd_key)as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.cat_id as product_name,
	pc.cat as category_id,	
	pc.subcat as category,
	pc.maintenance as subcategory,
	pn.prd_nm ,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null -- filter out all historical data
