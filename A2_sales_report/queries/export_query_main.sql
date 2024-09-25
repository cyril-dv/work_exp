select
	month_year, 
	global_sku_code, 
	global_pos_code, 
	pieces, 
	sales_rub
from
	retail_sales
where
	month_year = %s
	and network_name = %s