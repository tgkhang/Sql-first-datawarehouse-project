select 
cid, 
case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid) )
	else cid
end cid,
bdate,
gen 
from bronze.erp_cust_az12
where case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid) )
	else cid 
end not in (
select distinct cst_key from silver.crm_cust_info
)
-- CHECK OK 


-- birdth day check
-- check out of range date (ex: >100 y)
select distinct 
bdate
from bronze.erp_cust_az12
where bdate <'1925-01-01' or bdate > GETDATE()


-- transition for birthday update
select  
case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid) )
	else cid
end cid,
bdate,
case when bdate> GETDATE() then null
else bdate
end as bdate,
gen 
from bronze.erp_cust_az12


--gender
select distinct gen
from bronze.erp_cust_az12

select distinct 
gen, 
case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
	when upper(trim(gen)) in ('M', 'MALE') then 'Male'
	else 'n/a'
end as gen
from bronze.erp_cust_az12

-- FINAL transition
Insert into silver.erp_cust_az12
(
	cid, bdate, gen
)
select  
case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid) )
	else cid
end cid,
case when bdate> GETDATE() then null
else bdate
end as bdate,
case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
	when upper(trim(gen)) in ('M', 'MALE') then 'Male'
	else 'n/a'
end as gen
from bronze.erp_cust_az12