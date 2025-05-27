use Datawarehouse;
go


create or alter procedure bronze.load_bronze
as
BEGIN
	PRINT '================================================';
	PRINT 'Loading Bronze Layer';
	PRINT '================================================';

	PRINT '------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '------------------------------------------------';


	PRINT '>> Truncating Table: bronze.crm_cust_info';
	truncate table bronze.crm_cust_info
	PRINT '>> Inserting Data Into: bronze.crm_cust_info';
	bulk insert bronze.crm_cust_info
	from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_crm\cust_info.csv'
	with (
		FIRSTROW =2,
		FIELDTERMINATOR= ',',
		--imporove performance, when loading to db, lock table
		TABLOCK 
	);

	--SELECT * from bronze.crm_cust_info
	--SELECT count(*) from bronze.crm_cust_info

	PRINT '>> Truncating Table: bronze.crm_prd_info';
	truncate table bronze.crm_prd_info
	PRINT '>> Inserting Data Into: bronze.crm_prd_info';
	bulk insert bronze.crm_prd_info
	from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_crm\prd_info.csv'
	with (
		FIRSTROW =2,
		FIELDTERMINATOR= ',',
		--imporove performance, when loading to db, lock table
		TABLOCK 
	);
	--select * from bronze.crm_prd_info

	PRINT '>> Truncating Table: bronze.crm_sales_details';
	truncate table bronze.crm_sales_details
	PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
	bulk insert bronze.crm_sales_details
	from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_crm\sales_details.csv'
	with (
		FIRSTROW =2,
		FIELDTERMINATOR= ',',
		--imporove performance, when loading to db, lock table
		TABLOCK 
	);
	--select * from bronze.crm_sales_details


	PRINT '------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '------------------------------------------------';

	PRINT '>> Truncating Table: bronze.erp_cust_az12';
	truncate table bronze.erp_cust_az12
	PRINT '>> Insert Data into Table: bronze.erp_cust_az12';
	bulk insert bronze.erp_cust_az12
	from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with (
		FIRSTROW =2,
		FIELDTERMINATOR= ',',
		--imporove performance, when loading to db, lock table
		TABLOCK 
	);
	--select * from bronze.erp_cust_az12

	PRINT '>> Truncating Table: bronze.erp_loc_a101';
	truncate table bronze.erp_loc_a101
	PRINT '>> Insert Data into Table: bronze.erp_loc_a101';
	bulk insert bronze.erp_loc_a101
	from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_erp\LOC_A101.csv'
	with (
		FIRSTROW =2,
		FIELDTERMINATOR= ',',
		--imporove performance, when loading to db, lock table
		TABLOCK 
	);
	--select * from bronze.erp_loc_a101

	PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
	truncate table bronze.erp_px_cat_g1v2
	PRINT '>> Insert Data into Table: bronze.erp_px_cat_g1v2';
	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with (
		FIRSTROW =2,
		FIELDTERMINATOR= ',',
		--imporove performance, when loading to db, lock table
		TABLOCK 
	);
	--select * from bronze.erp_px_cat_g1v2

END;

exec bronze.load_bronze