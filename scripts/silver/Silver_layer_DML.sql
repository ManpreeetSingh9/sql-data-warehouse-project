/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure  silver.load_silver as
BEGIN

    PRINT '>> Truncating Table : silver.crm_cust_info ';
    TRUNCATE TABLE silver.crm_cust_info; 
    PRINT'>> Inserting Data into : silver.crm_cust_info';
    Insert into silver.crm_cust_info (
    cst_id ,
    cst_key ,
    cst_firstname ,
    cst_lastname ,
    cst_marital_status ,
    cst_gndr ,
    cst_create_date
    )
    select cst_id , 
    cst_key ,
    Trim(cst_firstname) as cst_firstname ,
    trim(cst_lastname) as cst_lastname ,
    case 
	     when  UPPER(TRIM( cst_marital_status)) = 'M' then 'Married' 
	     when  UPPER(TRIM( cst_marital_status)) = 'S' then 'Single'
	     else 'n/a'
     end cst_marital_status,
    case
	     when UPPER(TRIM(cst_gndr)) = 'M' then 'Male' 
	     when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
	     else 'n/a'
     end cst_gndr,
    cst_create_date from (select * ,
    row_number () over (partition by cst_id order by cst_create_date desc ) as flag_last
    from bronze.crm_cust_info  where cst_id is not null )t where flag_last =1 ;



    -- Inserting data to  bronze.crm_prd_info 

    PRINT '>> Truncating Table : silver.crm_prd_info ';
    TRUNCATE TABLE silver.crm_prd_info; 
    PRINT'>> Inserting Data into : silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (prd_id  , 
     cat_id ,
     prd_key ,
     prd_nm ,
     prd_cost ,
     prd_line , 
     prd_start_dt,
     prd_end_dt 
     ) 
    SELECT prd_id,
          REPLACE(SUBSTRING (prd_key , 1,5) , '-' , '_' )  cat_id,
          SUBSTRING (prd_key ,7,LEN(prd_key)) as prd_key 
          ,prd_nm
          , ISNULL(prd_cost ,0) as prd_cost,
          case UPPER(TRIM(prd_line))
            when 'M' then 'Mountain'
            when 'R' then 'Road'
            when 'S' then 'other Sales'
            when 'T' then 'Touring'
            else 'n/a' 
            end as prd_line
          ,cast (prd_start_dt as DATE) prd_start_dt
          ,CAST (LEAD(prd_start_dt) over (Partition by prd_key ORDER BY prd_start_dt ) -1 as DATE)  as  prd_end_dt
      
      FROM  bronze.crm_prd_info
  

      --Inserting into silver.crm_sales_details
    PRINT '>> Truncating Table : silver.crm_sales_details ';
    TRUNCATE TABLE silver.crm_sales_details; 
    PRINT'>> Inserting Data into : silver.crm_sales_details';
     INSERT INTO silver.crm_sales_details (
     sls_ord_num ,
     sls_prd_key ,
     sls_cust_id  ,
     sls_order_dt    ,
     sls_ship_dt   ,
     sls_due_dt  ,
     sls_sales  ,
     sls_quantity ,
     sls_price 
     )
     SELECT sls_ord_num,
          sls_prd_key,
          sls_cust_id,
          CASE 
            when sls_order_dt = 0 or LEN(sls_order_dt) != 8 then null
            else CAST(CAST(sls_order_dt as VARCHAR) as Date )
            end sls_order_dt ,
          CASE 
            when sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 then null
            else CAST(CAST(sls_ship_dt as VARCHAR) as Date )
            end sls_ship_dt ,
          CASE 
            when sls_due_dt = 0 or LEN(sls_due_dt) != 8 then null
            else CAST(CAST(sls_due_dt as VARCHAR) as Date )
            end sls_due_dt ,
      
          CASE 
            when sls_sales is null or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                then sls_quantity * ABS(sls_price)
            ELSE sls_sales 
            end sls_sales,
          sls_quantity,
          CASE 
            when sls_price is null or sls_price <= 0
                then sls_sales / NULLIF(sls_quantity , 0)
            else sls_price 
            end sls_price
      FROM  bronze.crm_sales_details ;
  

    -- INSERTING INTO   silver.erp_cust_az12
    PRINT '>> Truncating Table : silver.erp_cust_az12 ';
    TRUNCATE TABLE silver.erp_cust_az12; 
    PRINT'>> Inserting Data into : silver.erp_cust_az12';
     INSERT INTO silver.erp_cust_az12 (
     cid ,
     bdate  ,
     gen 
     )
    select 
      CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid , 4 , LEN(cid))
        ELSE cid 
        END cid ,
      CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
        END as bdate,
      CASE 
        WHEN UPPER(TRIM(gen)) in ('F' ,'FEMALE') THEN 'FEMALE'
        WHEN UPPER(TRIM(gen)) in ('M' ,'MALE') THEN 'MALE'
        ELSE 'n/a'
    END as gen
      from bronze.erp_cust_az12 ;

      -- INSERTING INTO silver.erp_loc_a101
    PRINT '>> Truncating Table : silver.erp_loc_a101 ';
    TRUNCATE TABLE silver.erp_loc_a101; 
    PRINT'>> Inserting Data into :silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (
    cid , cntry )
    SELECT 
    REPLACE (cid , '-' , '') cid ,
    CASE 
        WHEN Trim(cntry) ='DE' THEN 'Germany'
        WHEN TRIM (cntry) in ('US' , 'USA') then 'United States'
        WHEN  TRIM (cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END as cntry
    from bronze.erp_loc_a101;

    -- INSERTING INTO silver.erp_px_cat_g1v2
    PRINT '>> Truncating Table : silver.erp_px_cat_g1v2 ';
    TRUNCATE TABLE silver.erp_px_cat_g1v2; 
    PRINT'>> Inserting Data into :silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
    )
    SELECT 
    id,
    cat,
    subcat,
    maintenance
    FROM bronze.erp_px_cat_g1v2 ;
END 