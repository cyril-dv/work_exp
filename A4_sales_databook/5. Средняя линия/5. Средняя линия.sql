-- По группе
select 
    month_year, "group", count(global_pos_code) as akb, sum(skus) as skus
from (
    select 
        r.month_year, p."group", r.global_pos_code, count(distinct r.global_sku_code) as skus
    from
        retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    where 1=1
        and r.month_year >= '2023-01-01'
		and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
    group by r.month_year, p."group", r.global_pos_code 
) as d
group by month_year, "group"
order by month_year, "group"


--split
-- По группе и производителю
select 
    month_year, "group", producer, count(global_pos_code) as akb, sum(skus) as skus
from (
    select 
        r.month_year, p."group", p.producer, r.global_pos_code, count(distinct r.global_sku_code) as skus
    from
        retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    where 1=1
        and r.month_year >= '2023-01-01'
		and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
        and p.producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5')
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
    group by r.month_year, p."group", p.producer, r.global_pos_code
) as d
group by month_year, "group", producer
order by month_year, "group", producer


--split
-- По группе и формату магазинов
select 
    month_year, "group", store_format, count(global_pos_code) as akb, sum(skus) as skus
from (
    select 
        r.month_year, p."group", m.store_format_alt as store_format, r.global_pos_code, count(distinct r.global_sku_code) as skus
    from
        retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    join networks as m on r.global_pos_code = m.global_pos_code
    where 1=1
        and r.month_year >= '2023-01-01'
		and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
    group by r.month_year, p."group", m.store_format_alt, r.global_pos_code
) as d
group by month_year, "group", store_format
order by month_year, "group", store_format


--split
-- По группе, производителю и формату магазинов
select 
    month_year, "group", store_format, producer, count(global_pos_code) as akb, sum(skus) as skus
from (
    select 
        r.month_year, p."group", m.store_format_alt as store_format, p.producer, r.global_pos_code, count(distinct r.global_sku_code) as skus
    from
        retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    join networks as m on r.global_pos_code = m.global_pos_code 
    where 1=1
        and r.month_year >= '2023-01-01'
		and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
        and p.producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5')
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
    group by r.month_year, p."group", m.store_format_alt, p.producer, r.global_pos_code
) as d
group by month_year, "group", store_format, producer
order by month_year, "group", store_format, producer