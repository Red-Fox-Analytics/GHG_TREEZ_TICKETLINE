 with ticketline_data as (
	select   t.Storeid "Storeid",t.customer_uuid "Customer Uuid",t.product_id "Product Id",  t.ticketlineid "Ticketlineid",t.dateclosed , t.customname "Customname",t.age as "Age", t.age_group as "Age Group", t.approver "Approver", t.cashier "Cashier", t.cbd_perc "Dbd Perc", t.channel "Channel" ,t.classification "Classification"
		, t.county_name "County Name", t.customer_city "Customer City", t.customer_country "Customer Country", t.customer_groups "Customer Groups"
		, t.customer_signup_date "Customer Signup Date", t.customer_source "Customer Source", t.customer_state "Customer State", t.customer_type "Customer Type"
		,   t.distributor "Distributor", t.gender "Gender"
		, t.harvest_date "Harvest Date", t.inventory_date "Inventory Date", t.invoiceid "Invoiceid", t.last_visit "Last Visit", t.lat "Lat", t.lng "Lng", t.package_size "Package Size"
		,concat(t.productsubtype||' ',t."SIZE") "Product Subtype + Size", t.productattributes "Productattributes", t.productbrand "Productbrand"
		, t.productsubtype "Productsubtype", t.producttype "Producttype", t.region "Region", t.retail_brand "Retail Brand", t."SIZE" "Size"
		, t.sku "Sku", t.state_name "State Name", t.store_state "Store State", t.thc_perc "Thc Perc", t.thc_ratio "Thc Ratio"
		, case when t.ticket_type='DELIVERY' then 'Delivery' else 'In-Store' end  "Ticket Type (group)"
		, case when t.register in ('DELIVERY 01','DELIVERY 02','DELIVERY 03','DELIVERY POUCH') then 'Delivery  	(Pottery)' else 'In-Store (Pottery)' end  "Ticket Type (Pottery)"
		, t.ticket_updated_date "Ticket Update Date", t.ticketid "Ticketid" , t.tier "Tier", t.total_mg_cbd "Total Mg Cbd" 
		, t.total_mg_thc "Total Mg Thc", t."TYPE" "Type", t.unitofmeasure "Unitofmeasure", t.weight "Weight", t.zip "Zip", t.productname "Productname", t.ticketalphaid "Ticketalphaid"
		, t."_FIVETRAN_BATCH" "Fivertran Batch", t."_FIVETRAN_INDEX" "Fivertran Index", t."_FIVETRAN_SYNCED" "Fivertran Synced",  concat(t."SIZE"||' ', t.package_size) "Size/Pack Size(Custom)"
		--, t.cost "Cost", t.discounts "Discounts", t.excisetaxamount  "Escisetaxamount", t.income_household_median "Income Household Median" 
		--, t.netsales "Netsales", t.qty "Qty", t."returns" "Returns", t.reward_balance "Reward Balance"
	from {{source('FIVETRAN_DATABASE','ticketline')}} t--treez_fivetran.ticketline t 
), ticketline_aggr as 
(
	select STOREID "Storeid",customer_uuid "Customer Uuid",product_id "Product Id", dateclosed 
		   ,  sum(cost) "Cost", sum(discounts) "Discounts", sum(excisetaxamount)  "Escisetaxamount", sum(income_household_median) "Income Household Median" 
		, sum(netsales) "Netsales", sum(qty) "Qty", sum("RETURNS") "Returns", sum(reward_balance) "Reward Balance"
	from treez_fivetran.ticketline
	group by 1,2,3,4
	--limit 5
), ticketline_agg_cmb as 
(
	select td.*, ta."Cost", ta."Discounts", ta."Escisetaxamount", ta."Income Household Median" ,ta."Netsales", ta."Qty", ta."Returns", ta."Reward Balance"
	from ticketline_aggr ta
	left join ticketline_data td 
	on ta."Storeid"= td."Storeid" and ta."Customer Uuid"=td."Customer Uuid" and ta."Product Id"=td."Product Id" and  ta.DATECLOSED= td.DATECLOSED
)

select td.* , td1."Cost" "Cost Ya", td1."Discounts" "Discounts Ya", td1."Escisetaxamount"  "Escisetaxamount Ya",td1."Income Household Median" "Income Household Median Ya" 
		, td1."Netsales" "Netsales Ya", td1."Qty" "Qty Ya", td1."Returns" "Returns Ya",td1."Reward Balance" "Reward Balance Ya", td1."DATECLOSED" "dateclosed"
from ticketline_agg_cmb td
left join ticketline_agg_cmb td1 
on  td."Customer Uuid" = td1."Customer Uuid"
	and td."Storeid" = td1."Storeid"
	and td."Product Id" = td1."Product Id"
	and date_part('day',td."DATECLOSED" ::date)=date_part('day',td1."DATECLOSED"::date) 
	and date_part('month',td."DATECLOSED" ::date)=date_part('month',td1."DATECLOSED"::date) 
    and date_part('year',td."DATECLOSED"::date)=date_part('year',td1."DATECLOSED"::date)+1

    