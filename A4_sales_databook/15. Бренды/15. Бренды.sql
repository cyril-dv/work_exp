select 
    report_date, "group", producer, brand, 
    case when producer = brand then tonnes+0.0001 else tonnes end as tonnes, 
    case when producer = brand then thou_rub+0.0001 else thou_rub end as thou_rub
from (
    (
    select
        r.month_year as report_date, p."group", p.producer, coalesce(brand, producer) as brand, sum(r.pieces * p.weight / 1000000) as tonnes, sum(r.sales_rub/1000) as thou_rub
    from retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    where r.month_year >= '2023-01-01' and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5') and (
        (p."group" = 'group_1' and producer in (
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4', 
                'producer_5',
                'producer_6',
                'producer_7'
            )
        )
        or (p."group" = 'group_2' and producer in (
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4', 
                'producer_5',
                'producer_6',
                'producer_7',
                'producer_8',
                'producer_9'
            )
        )
        or (p."group" = 'group_3' and producer in (
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4' 
            )
        )
        or (p."group" = 'group_4' and producer in (
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4', 
                'producer_5',
                'producer_6',
                'producer_7',
                'producer_8',
                'producer_9'
            )
        )
        or (p."group" = 'group_5' and producer in (
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4', 
                'producer_5',
                'producer_6',
                'producer_7',
                'producer_8',
                'producer_9'      
            )
        )
        or (p."group" = 'group_6' and producer in (
                'producer_1', 
                'producer_2', 
                'producer_3'
            )
        )
    )
    group by grouping sets (
        (r.month_year, p."group", p.producer, p.brand),
        (r.month_year, p."group", p.producer))
    )
union
    (
    select
        r.month_year as report_date, p."group", 'Прочие' as producer, 'Прочие' as brand, sum(r.pieces * p.weight / 1000000) as tonnes, sum(r.sales_rub/1000) as thou_rub
    from retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    where r.month_year >= '2023-01-01' and r.network_name not in 'network_1', 'network_2', 'network_3', 'network_4', 'network_5') and (
        (p."group" = 'group_1' and producer not in (
                'producer_A', 'producer_B', 'producer_C', 'producer_D', 'producer_E', 'producer_F', 'producer_G', 'producer_H',
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4', 
                'producer_5',
                'producer_6',
                'producer_7'
            )
        )
        or (p."group" = 'group_2' and producer not in (
                'producer_A', 'producer_B', 'producer_C', 'producer_D', 'producer_E', 'producer_F', 'producer_G', 'producer_H',
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4', 
                'producer_5',
                'producer_6',
                'producer_7',
                'producer_8',
                'producer_9'
            )
        )
        or (p."group" = 'group_3' and producer not in (
                'producer_A', 'producer_B', 'producer_C', 'producer_D', 'producer_E', 'producer_F', 'producer_G', 'producer_H',
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4'
            )
        )
        or (p."group" = 'group_4' and producer not in (
                'producer_A', 'producer_B', 'producer_C', 'producer_D', 'producer_E', 'producer_F', 'producer_G', 'producer_H',
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4', 
                'producer_5',
                'producer_6',
                'producer_7',
                'producer_8',
                'producer_9'
            )
        )
        or (p."group" = 'group_5' and producer not in (
                'producer_A', 'producer_B', 'producer_C', 'producer_D', 'producer_E', 'producer_F', 'producer_G', 'producer_H',
                'producer_1', 
                'producer_2', 
                'producer_3', 
                'producer_4', 
                'producer_5',
                'producer_6',
                'producer_7',
                'producer_8',
                'producer_9'
            )
        )
        or (p."group" = 'group_6' and producer not in (
                'producer_A', 'producer_B', 'producer_C', 'producer_D', 'producer_E', 'producer_F', 'producer_G', 'producer_H',
                'producer_1', 
                'producer_2', 
                'producer_3'
            )
        )
    )
    group by r.month_year, p."group"
    )
union
    (
    select
        r.month_year as report_date, p."group", 'СТМ' as producer, 'СТМ' as brand, sum(r.pieces * p.weight / 1000000) as tonnes, sum(r.sales_rub/1000) as thou_rub
    from retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    where r.month_year >= '2023-01-01' and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
        and p.producer in ('producer_A', 'producer_B', 'producer_C', 'producer_D', 'producer_E', 'producer_F', 'producer_G', 'producer_H')
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
    group by r.month_year, p."group"
    )
) as q
order by "group", producer, brand, report_date