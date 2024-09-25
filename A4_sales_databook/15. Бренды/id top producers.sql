select 
    row_number() over (partition by "group" order by tn_perc desc, tn desc) as pos, 
    "group", 
    producer, 
    round(tn::numeric, 1) as tn, 
    round(tn_total::numeric, 1) as tn_total, 
    round(tn_perc::numeric, 1) as tn_perc, 
    round(sum(tn_perc::numeric) over (partition by "group" order by tn_perc desc, tn desc), 1) as tn_perc_cumsum
from (
    select
        "group", producer, tn, tn_total, (tn/tn_total*100) as tn_perc
    from (
        select "group", producer, tn, sum(tn) over (partition by "group") as tn_total
        from (
            select p."group", case when left(p.producer, 4) = 'XXX' then 'XXX' else p.producer end as producer, sum(r.pieces * p.weight / 1000000) as tn
            from
                retail_sales as r
                join base_sku as p on r.global_sku_code = p.global_sku_code
            where 1=1
                and r.month_year between ('2024-05-01'::date - '2 months'::interval)::date and '2024-05-01'
                and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
                and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
            group by p."group", case when left(p.producer, 4) = 'XXX' then 'XXX' else p.producer end
        ) as q1
    ) as q2
) as q3
where round(tn_perc::numeric, 1) >= 1