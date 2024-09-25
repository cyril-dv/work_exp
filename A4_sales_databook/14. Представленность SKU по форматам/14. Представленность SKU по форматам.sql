select 
    akb_sku.store_format,
    akb_sku."group",
    akb_sku.producer,
    akb_sku.global_sku_name,
    akb_sku.akb_sku,
    akb_group.akb_grp,
    akb_sku.kg_sku
from 
    (
    select
        m.store_format_alt as store_format,
        p."group",
        count(distinct n.global_pos_code) as akb_grp
    from
        retail_sales as r
        join base_sku as p on r.global_sku_code = p.global_sku_code
        join base_sales_point as n on r.global_pos_code = n.global_pos_code
        join networks as m on r.global_pos_code = m.global_pos_code 
    where 1=1
        and r.month_year = '2024-06-01'
        and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4')
    group by
        m.store_format_alt,
        p."group"
    ) as akb_group
join 
    (
    select
        m.store_format_alt as store_format,
        p."group",
        p.producer,
        p.global_sku_name,
        count(distinct n.global_pos_code) as akb_sku,
        sum(r.pieces * p.weight / 1000) as kg_sku
    from
        retail_sales as r
        join base_sku as p on r.global_sku_code = p.global_sku_code
        join base_sales_point as n on r.global_pos_code = n.global_pos_code
        join networks as m on r.global_pos_code = m.global_pos_code 
    where 1=1
        and r.month_year = '2024-06-01'
		and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4')
    group by
        m.store_format_alt,
        p."group",
        p.producer,
        p.global_sku_name
    ) as akb_sku
on akb_group.store_format = akb_sku.store_format and akb_group."group" = akb_sku."group"
where 1=1
    and case when (akb_sku.akb_sku::float / akb_group.akb_grp::float) > 0.1 then 1 else 0 end = 1