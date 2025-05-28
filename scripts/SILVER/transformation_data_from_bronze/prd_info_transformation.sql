select 
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
From bronze.crm_prd_info


-- crm.prd_info check
-- Check for null and duplicate primary key
-- Expect: no result

select prd_id, count(*) as "nums appearance" 
from bronze.crm_prd_info
group by prd_id
having count(*)>1 or prd_id IS NULL

-- check category key
select 
prd_id,
prd_key,
REPlACE(SUBSTRING(prd_key, 1,5),'-','_') as cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
From bronze.crm_prd_info
where REPlACE(SUBSTRING(prd_key, 1,5),'-','_') not in
(select distinct id from bronze.erp_px_cat_g1v2)

-- check for product key
select 
prd_id,
prd_key,
REPlACE(SUBSTRING(prd_key, 1,5),'-','_') as cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
From bronze.crm_prd_info
where SUBSTRING(prd_key, 7, LEN(prd_key)) not IN
(select sls_prd_key from bronze.crm_sales_details)


-- Check for unwanted spaces
-- Expected no result
select prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- check fo rnulls or negative number
-- expect no res
-- if have replace that with 0 in transformation
select prd_cost
from bronze.crm_prd_info
where prd_cost<0 or prd_cost is null

-- replace cost with 0 if it is null or negative
select 
prd_id,
prd_key,
REPlACE(SUBSTRING(prd_key, 1,5),'-','_') as cat_id,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
From bronze.crm_prd_info

-- prd_line
select distinct prd_line
from bronze.crm_prd_info

-- replace
select 
prd_id,
prd_key,
REPlACE(SUBSTRING(prd_key, 1,5),'-','_') as cat_id,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
case UPPER(TRIM(prd_line))
	when 'M' then 'Mountain'
	when 'R' then 'Road'
	when 'S' then 'Other Sales'
	when 'T' then 'Touring'
	ELSE 'n/a'
end as prd_line,
prd_start_dt,
prd_end_dt
From bronze.crm_prd_info


-- start and end date
-- check for invalid data order
select * 
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

--test for end date next chosing
select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) Over (partition by prd_key ORDER by prd_start_dt)-1 as prd_end_dt_test	
from bronze.crm_prd_info
where prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- FINAL TRANSFORMATION
insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
select 
prd_id,
prd_key,
REPlACE(SUBSTRING(prd_key, 1,5),'-','_') as cat_id,  -- extract category id
--SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_id,       -- extract prodcut key
prd_nm,												
ISNULL(prd_cost,0) as prd_cost,
case UPPER(TRIM(prd_line))
	when 'M' then 'Mountain'
	when 'R' then 'Road'
	when 'S' then 'Other Sales'
	when 'T' then 'Touring'
	ELSE 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast( LEAD(prd_start_dt) Over (partition by prd_key ORDER by prd_start_dt)-1 as date) as prd_end_dt -- caculate end date as one day before next start date
From bronze.crm_prd_info