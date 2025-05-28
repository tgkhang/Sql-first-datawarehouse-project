select 
id,
cat, 
subcat,
maintenance
from bronze.erp_px_cat_g1v2


select cat_id
from silver.crm_prd_info

-- check unwanted space
select * from 
bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat)
or maintenance != trim(maintenance)
-- RESULLT : ok

--chekc consistency
select distinct cat
from bronze.erp_px_cat_g1v2

select distinct subcat
from bronze.erp_px_cat_g1v2

select distinct maintenance
from bronze.erp_px_cat_g1v2
--RESULT: nice

--FINAl
insert into silver.erp_px_cat_g1v2
(
id,
cat, 
subcat,
maintenance
)select 
id,
cat, 
subcat,
maintenance
from bronze.erp_px_cat_g1v2