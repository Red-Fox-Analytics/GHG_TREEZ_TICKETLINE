WITH ticketline AS 
(
 	select   t.Storeid "Storeid",t.customer_uuid "Customer Uuid",t.product_id "Product Id",  t.ticketlineid "Ticketlineid",t.dateclosed , t.customname "Customname",t.age as "Age", t.age_group as "Age Group", t.approver "Approver", t.cashier "Cashier", t.cbd_perc "Dbd Perc", t.channel "Channel" ,t.classification "Classification"
		, t.county_name "County Name", t.customer_city "Customer City", t.customer_country "Customer Country", t.customer_groups "Customer Groups"
		, t.customer_signup_date "Customer Signup Date", t.customer_source "Customer Source", t.customer_state "Customer State", t.customer_type "Customer Type"
		,   t.distributor "Distributor", t.gender "Gender"
		, t.harvest_date "Harvest Date", t.inventory_date "Inventory Date", t.invoiceid "Invoiceid", t.last_visit "Last Visit", t.lat "Lat", t.lng "Lng", t.package_size "Package Size"
		, concat(t.productsubtype||' ',t."SIZE") "Product Subtype + Size", t.productattributes "Productattributes"
		, case when t.productbrand is null or t.productbrand='' then 'BRAND NOT CODED' else t.productbrand end "Productbrand"
		, t.productsubtype "Productsubtype", t.producttype "Producttype", t.region "Region"
		, case when t.retail_brand is null or t.retail_brand='' then 'BRAND NOT CODED' else t.retail_brand end "Retail Brand", t."SIZE" "Size"
		, t.sku "Sku", t.state_name "State Name", t.store_state "Store State", t.thc_perc "Thc Perc", t.thc_ratio "Thc Ratio"
		, case when t.ticket_type='DELIVERY' then 'Delivery' else 'In-Store' end  "Ticket Type (group)"
		, case when t.register in ('DELIVERY 01','DELIVERY 02','DELIVERY 03','DELIVERY POUCH') then 'Delivery (Pottery)' else 'In-Store (Pottery)' end  "Ticket Type (Pottery)"
		, t.ticket_updated_date "Ticket Update Date", t.ticketid "Ticketid" , t.tier "Tier", t.total_mg_cbd "Total Mg Cbd" 
		, t.total_mg_thc "Total Mg Thc", t."TYPE" "Type", t.unitofmeasure "Unitofmeasure", t.weight "Weight", t.zip "Zip", t.productname "Productname", t.ticketalphaid "Ticketalphaid"
		, t."_FIVETRAN_BATCH" "Fivertran Batch", t."_FIVETRAN_INDEX" "Fivertran Index", t."_FIVETRAN_SYNCED" "Fivertran Synced",  concat(t."SIZE"||' ', t.package_size) "Size/Pack Size(Custom)"
		, t.cost "Cost", t.discounts "Discounts", t.excisetaxamount  "Escisetaxamount", t.income_household_median "Income Household Median" 
		, t.netsales "Netsales", t.qty "Qty", t."RETURNS" "Returns", t.reward_balance "Reward Balance"
	from {{source('FIVETRAN_DATABASE','ticketline')}} t--treez_fivetran.ticketline t-- t-- t 
), maxdate_cte AS 
(
	SELECT max(cast(DATECLOSED AS date)) MAX_DATE FROM TICKETLINE  	
)
SELECT *, (SELECT MAX_DATE FROM maxdate_cte) MAX_DATE FROM ticketline