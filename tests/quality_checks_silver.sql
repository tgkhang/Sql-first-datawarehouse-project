-- Check for null and duplicate primary key
-- Expect: no result

select cst_id, count(*) as "nums appearance" 
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id IS NULL


-- Check for unwanted spaces
-- Expected no result
select cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

-- Data Standarization and Consistency check
select Distinct cst_gndr
from silver.crm_cust_info

select distinct cst_marital_status
from silver.crm_cust_info