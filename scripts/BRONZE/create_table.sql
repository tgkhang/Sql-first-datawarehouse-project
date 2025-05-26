
use Datawarehouse;
go


IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

/*notice name convention */
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
go


IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info(
prd_id Int,
prd_key Nvarchar(50),
prd_nm nvarchar(50),
prd_cost INT,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime

);
go

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
Drop table  bronze.crm_sales_details;
go

create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);
go;

 
IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101;

GO

CREATE TABLE bronze.erp_loc_a101 
(
    cid NVARCHAR (50), 
    cntry NVARCHAR (50)
);

GO
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
DROP TABLE bronze.erp_cust_az12;

GO
CREATE TABLE
    bronze.erp_cust_az12 (cid NVARCHAR (50), bdate DATE, gen NVARCHAR (50));

GO 
IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
DROP TABLE bronze.erp_px_cat_g1v2;

GO
CREATE TABLE
    bronze.erp_px_cat_g1v2 (
        id NVARCHAR (50),
        cat NVARCHAR (50),
        subcat NVARCHAR (50),
        maintenance NVARCHAR (50)
    );

GO