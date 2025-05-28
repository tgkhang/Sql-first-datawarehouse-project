-- Check for null and duplicate primary key
-- Expect: no result

select cst_id, count(*) as "nums appearance" 
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id IS NULL


-- Check for unwanted spaces
-- Expected no result
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)


-- Data Standarization and Consistency check
select Distinct cst_gndr
from bronze.crm_cust_info

select distinct cst_marital_status
from bronze.crm_cust_info


--transformation
select 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
	else 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
	else 'n/a'
END cst_gndr,
cst_create_date
from(
	-- add flag
	select *, ROW_NUMBER() OVER (PARTITION BY cst_id order by cst_create_date DESC) as flag_last
	from bronze.crm_cust_info
	--where cst_id= 29449
) t
where flag_last =1 
--AND cst_id = 29466



-- INSERT the transition table to silver
insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)
select 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
	else 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
	else 'n/a'
END cst_gndr,
cst_create_date
from(
	-- add flag
	select *, ROW_NUMBER() OVER (PARTITION BY cst_id order by cst_create_date DESC) as flag_last
	from bronze.crm_cust_info
	--where cst_id= 29449
) t
where flag_last =1 