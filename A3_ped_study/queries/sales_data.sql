select
	extract(week from r.week_start_date) as wk_num,
	r.week_start_date,
	r.global_pos_code,
	p.global_sku_code,
	sum(r.pieces * p.weight / 1000.0) as kg,
	sum(r.sales_rub) as rub,
	sum(r.sales_rub)/sum(r.pieces*p.weight/1000.0) as price_kg,
	floor(sum(r.sales_rub)/sum(r.pieces*p.weight/1000.0)/5.00)*5 as price_bin
from retail_sales as r 
join base_sku as p on r.global_sku_code = p.global_sku_code 
join base_sales_point as n on r.global_pos_code = n.global_pos_code
join networks as m on r.global_pos_code = m.global_pos_code
where 1=1
	and m.network_subname_alt = ?
	and r.global_sku_code = ?
	and r.week_start_date between ? and ?
group by
	extract(week from r.week_start_date),
	r.week_start_date,
	r.global_pos_code,
	p.global_sku_code