select 
	r.month_year as report_date,
	n.network_name,
	f.network_subname_alt as network_subname,
	p.category,
	p.segment,
	p."group",
	p.producer,
	p.brand,
	p.global_sku_name,
	coalesce(p.weight, 0) as weight,
	count(distinct r.global_pos_code) as akb,
	sum(r.pieces) as pieces,
	sum(r.pieces * p.weight / 1000) as kg,
	sum(r.sales_rub) as sales_rub
from
	retail_sales as r
	join base_sku as p on r.global_sku_code = p.global_sku_code 
	join base_sales_point as n on r.global_pos_code = n.global_pos_code
	join networks as f on r.global_pos_code = f.global_pos_code
group by grouping sets 
	(
		(r.month_year, n.network_name, f.network_subname_alt, p.category, p.segment, p."group", p.producer, p.brand, p.global_sku_name, p.weight),
		(r.month_year, n.network_name, f.network_subname_alt, p.category, p.segment, p."group", p.producer),
		(r.month_year, n.network_name, f.network_subname_alt, p.category, p.segment, p."group"),
		(r.month_year, n.network_name, f.network_subname_alt)     
	)