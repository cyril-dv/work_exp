with dataset as (
    select p."group", p.brand, p.weight, floor((r.sales_rub / nullif(r.pieces * p.weight / 1000, 0))/20.00)*20 AS bin, r.pieces, (r.pieces * p.weight / 1000) as kg
    from retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    join base_sales_point as n on r.global_pos_code = n.global_pos_code
    where 1=1
        and r.month_year between '2024-04-01' and '2024-06-01'
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
        and p."group" = ?
)
select "group", brand, weight, bin, bin_range, pieces, pcs_total, coalesce((pieces / nullif(pcs_total, 0)), 0) as pct
from
    (
    select "group", brand, weight, bin, bin_range, pieces, sum(pieces) over(partition by "group", brand, weight) as pcs_total
    from 
        (
        select p."group", p.brand, p.weight, b.bin, b.bin::int::varchar || '-' || (b.bin + 20)::int::varchar as bin_range, coalesce(s.pieces, 0) as pieces
        from 
            (
            select bin 
            from 
                (
                select bin, kg, sum(kg) over () as kg_total 
                from 
                    (
                    select bin, sum(kg) as kg 
                    from dataset 
                    group by bin
                    ) as q1
                ) as q2 
                where (kg / kg_total) > 0.03
            ) as b
        cross join    
            (    
            select "group", brand, weight
            from 
                (
                select "group", brand, weight, kg, sum(kg) over (order by kg) as kg_cumsum, sum(kg) over () as kg_total
                from 
                    (
                    select "group", brand, weight, sum(kg) as kg
                    from dataset
                    group by "group", brand, weight
                    ) as q1
                ) as q2
            where round((kg_cumsum / kg_total)::numeric, 2) >= ?
            ) as p
        left join
            (
            select "group", brand, weight, bin, sum(pieces) as pieces
            from dataset
            group by "group", brand, weight, bin
            ) as s
        on p."group" = s."group" and p.brand = s.brand and p.weight = s.weight and b.bin = s.bin
        ) as q1
    ) as q2
order by "group", brand, weight, bin