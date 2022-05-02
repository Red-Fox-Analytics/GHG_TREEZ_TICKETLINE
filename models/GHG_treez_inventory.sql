 SELECT
	i.BATCH_ID , i.LOCATION_ID , i.PRODUCT_ID , i.STATE_TRACKING_ID , i.STOREID , i.LAST_USER_ID , i."SOURCE" , i.PACKAGE_ID 
	, i."TYPE" , i.EXPIRATION_DATE ,i.PRODUCTTYPE , i.LAST_RETRIEVED_TIMESTAMP , i.LAST_RECONCILIATION_ID , i.INVOICE_ID 
	, i.ID , i.ORIGINAL_START_DATE , i.INVOICE_LINE_ID , i.LOCATION_NAME , i.IS_SELLABLE , i.DISTRIBUTOR_ID , i.LAST_LOCATION_ID 
	, i.INVENTORY_TYPE , i.STATUS , i.LAST_ACTION , i.PRODUCT_SKU , i.DATALAKE_DATE , i.UNIT_OF_MEASURE , i.PRODUCTNAME 
	, i.ACCEPTED_DATE , i.PRODUCTBRAND , i.START_DATE , i.OG_PRODUCTNAME , i.LOCATION_TYPE , i.LAST_INVENTORY_ID , i.HARVEST_DATE 
	, i.RETAIL_BRAND , i.PRODUCT_EXTERNAL_ID , i."_FIVETRAN_SYNCED" 
	, i.CHANGE_QUANTITY , i.PACKED_AND_READY_UNITS , i.ADJUSTED_UNITS , i.DECISION_PENDING_UNITS , i.AVAILABLE_UNITS 
	, i.PRICEUNIT , i.SOLD_UNITS , i.CONVERTED_UNITS , i.EXCISE_TAX_PER_UNIT , i.DESTROYED_UNITS , i.RESERVED_UNITS 
	, i.RETURNED_FROM_POS , i.RETURNED_UNITS , i.COST_PER_UNIT 
FROM {{source('FIVETRAN_DATABASE','INVENTORY')}} i