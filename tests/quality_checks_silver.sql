--CRM CUST INFO TABLE CHECK

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

--============================
-- CRM PRD INFO TABLE CHECK
select prd_id, count(*) as "nums appearance" 
from silver.crm_prd_info
group by prd_id
having count(*)>1 or prd_id IS NULL


-- Check for unwanted spaces
-- Expected no result
select prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- check fo rnulls or negative number
-- expect no res
-- if have replace that with 0 in transformation
select prd_cost
from silver.crm_prd_info
where prd_cost<0 or prd_cost is null

select distinct prd_line
from silver.crm_prd_info


-- .erp_cust_az12 check silver
select distinct 
bdate
from silver.erp_cust_az12
where bdate <'1925-01-01' or bdate > GETDATE()

select distinct gen
from silver.erp_cust_az12

select * from silver.erp_cust_az12