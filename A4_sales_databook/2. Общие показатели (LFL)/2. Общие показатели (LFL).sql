select
    r.month_year as report_date,
    p."group",
    sum(r.pieces * p.weight / 1000000) as tonnes,
    sum(r.pieces / 1000.00) as thou_pcs,
    sum(r.sales_rub / 1000000) as mil_rub
from retail_sales as r
join base_sku as p on r.global_sku_code = p.global_sku_code
where 1=1
    and r.month_year >= '2023-01-01'
    and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
    and r.global_pos_code in (    
                                select j.global_pos_code
                                from 
                                    (
                                    select distinct r.global_pos_code 
                                    from retail_sales as r
                                    join base_sales_point as n on r.global_pos_code = n.global_pos_code  
                                    where r.month_year = '2023-06-01' and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
                                    ) as j
                                join
                                    (
                                    select distinct r.global_pos_code 
                                    from retail_sales as r
                                    join base_sales_point as n on r.global_pos_code = n.global_pos_code  
                                    where r.month_year = '2024-06-01' and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
                                    ) as d 
                                on j.global_pos_code = d.global_pos_code
                            )
group by 
    r.month_year,
    p."group"
order by 
    "group",
    report_date