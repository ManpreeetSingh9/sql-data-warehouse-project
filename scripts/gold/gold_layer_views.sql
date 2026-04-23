/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================


IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

-- FIRST view by joining three tables --> [silver.crm_cust_info ,silver.erp_cust_az12 ,silver.erp_loc_a101]

CREATE VIEW gold.dim_customers  as
	SELECT 
	ROW_NUMBER () over (order by cst_id ) as customer_key,
	ci.cst_id as customer_id ,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country ,
	ci.cst_marital_status as marital_status,
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		ELSE COALESCE(ca.gen , 'n/a')
	END as gender ,
	ca.bdate as birthdate   ,
	ci.cst_create_date as create_date

	FROM silver.crm_cust_info ci 
	LEFT JOIN silver.erp_cust_az12 ca 
	on ci.cst_key = ca.cid 
	LEFT JOIN silver.erp_loc_a101 la
	on ci.cst_key = la.cid ;

GO

-- SECOND view by joining tables --> [ silver.crm_prd_info , silver.erp_px_cat_g1v2 ]

CREATE VIEW gold.dim_products as 
	SELECT 
	ROW_NUMBER () over (order by pn.prd_start_dt , pn.prd_key) as product_key ,
	prd_id as product_id,
	prd_key as product_number,
	prd_nm as product_name,
	cat_id as category_id,
	pc.cat  as category,
	pc.subcat as subcategory,
	pc.maintenance ,
	prd_cost as cost,
	prd_line as product_line,
	prd_start_dt as start_date
	FROM silver.crm_prd_info pn 
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	on pn.cat_id = pc.id
	where prd_end_dt IS NULL ;

	GO
-- FACT view for integrity using table crm_sales_details

CREATE VIEW gold.fact_sales as 
	SELECT 
	sd.sls_ord_num as order_number  ,
	pr.product_key, 
	cu.customer_key ,
	sd.sls_order_dt as order_date ,
	sd.sls_ship_dt as shipping_date ,
	sd.sls_due_dt  as due_date,
	sd.sls_sales  as sales_amount  ,
	sd.sls_quantity  as quantity,
	sd.sls_price as price 
	FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_products pr
	on sd.sls_prd_key = pr.product_number 
	LEFT JOIN gold.dim_customers cu 
	on sd.sls_cust_id = cu.customer_id;

