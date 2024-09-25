select report_date, network_subname, "group", producer, sum(tn) as tn
from 
    (    
    select 
        extract('year' from r.month_year) as report_date,
        case
            when r.network_subname in ('___') then 'network_1'
            when r.network_subname in ('___') then 'network_2'
            else r.network_subname
        end as network_subname,
        p."group",
        'Все производители' as producer,
        sum(r.pieces * p.weight / 1000000) as tn
    from 
        retail_sales as r
        join base_sku as p on r.global_sku_code = p.global_sku_code
        where 1=1
            and (r.month_year between '2023-01-01' and '2023-06-01' or r.month_year between '2024-01-01' and '2024-06-01')
            and p."group" in ('group_1', 'group_2', 'group_3')
            and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
    group by 
        extract('year' from r.month_year),
        case
            when r.network_subname in ('___') then 'network_1'
            when r.network_subname in ('___') then 'network_2'
            else r.network_subname
        end,
        p."group"
union
    select 
        extract('year' from r.month_year) as report_date,
        case
            when r.network_subname in ('___') then 'network_1'
            when r.network_subname in ('___') then 'network_2'
            else r.network_subname
        end as network_subname,
        p."group",
        'Нэфис Косметикс' as producer,
        sum(r.pieces * p.weight / 1000000) as tn
    from 
        retail_sales as r
        join base_sku as p on r.global_sku_code = p.global_sku_code
        where 1=1
            and (r.month_year between '2023-01-01' and '2023-06-01' or r.month_year between '2024-01-01' and '2024-06-01')
            and p."group" in ('group_1', 'group_2', 'group_3')
            and p.producer = 'XX'
            and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
    group by 
        extract('year' from r.month_year),
        case
            when r.network_subname in ('___') then 'network_1'
            when r.network_subname in ('___') then 'network_2'
            else r.network_subname
        end,
        p."group"
union
    select 
        extract('year' from r.month_year) as report_date,
        case
            when r.network_subname in ('___') then 'network_1'
            when r.network_subname in ('___') then 'network_2'
            else r.network_subname
        end as network_subname,
        p."group",
        'Другие производители' as producer,
        sum(r.pieces * p.weight / 1000000) as tn
    from 
        retail_sales as r
        join base_sku as p on r.global_sku_code = p.global_sku_code
        where 1=1
            and (r.month_year between '2023-01-01' and '2023-06-01' or r.month_year between '2024-01-01' and '2024-06-01')
            and p."group" in ('group_1', 'group_2', 'group_3')
            and p.producer <> 'XX'
            and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
    group by 
        extract('year' from r.month_year),
        case
            when r.network_subname in ('___') then 'network_1'
            when r.network_subname in ('___') then 'network_2'
            else r.network_subname
        end,
        p."group"
    ) as q
group by grouping sets 
    (
    (report_date, network_subname, "group", producer),
    (report_date, "group", producer),
    (report_date, producer)
    )