select 
cid, cntry
from bronze.erp_loc_a101;

select cst_key from silver.crm_cust_info
-- has minus in the erp

select 
replace(cid,'-','') cid,
cntry
from bronze.erp_loc_a101
where replace(cid,'-','') not in 
(
	select cst_key from silver.crm_cust_info
)
-- res: match data ok

-- check country
select distinct 
cntry 
from bronze.erp_loc_a101
order by cntry

-- tranformation
select distinct 
cntry as 'old-cntry',
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US', 'USA') then 'United States'
	when trim(cntry)= '' or cntry is null then 'n/a'
else trim(cntry)
end as cntry
from bronze.erp_loc_a101
order by cntry

--FINAL transition
insert into silver.erp_loc_a101(
cid, cntry
)
select 
replace(cid,'-','') cid,
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US', 'USA') then 'United States'
	when trim(cntry)= '' or cntry is null then 'n/a'
else trim(cntry)
end as cntry
from bronze.erp_loc_a101