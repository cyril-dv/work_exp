select 
    coalesce(curr.network_subname, prev.network_subname) as network_subname,
    coalesce(curr."group", prev."group") as "group",
    coalesce(curr.producer, prev.producer) as producer,
    coalesce(curr.tn_perc, 0) as tn_curr, 
    coalesce(prev.tn_perc, 0) as tn_prev,
    coalesce(curr.tn, 0) as tn_sort,
    case 
       when coalesce(curr.tn_perc, 0) - coalesce(prev.tn_perc, 0) > 0 then concat(coalesce(curr.tn_perc, 0), ' (+', coalesce(curr.tn_perc, 0)::int-coalesce(prev.tn_perc, 0)::int, ')')
       when coalesce(curr.tn_perc, 0) - coalesce(prev.tn_perc, 0) < 0 then concat(coalesce(curr.tn_perc, 0), ' (', coalesce(curr.tn_perc, 0)::int-coalesce(prev.tn_perc, 0)::int, ')')
       when coalesce(curr.tn_perc, 0) - coalesce(prev.tn_perc, 0) = 0 then concat(coalesce(curr.tn_perc, 0), ' (0)')
    end as tn_change
from 
    (
    select
        network_subname, "group", producer, tn, tn_total, round((tn/nullif(tn_total, 0)*100))::int as tn_perc
    from
        (
        select network_subname, producer, "group", tn, sum(tn) over (partition by network_subname, "group") as tn_total
        from 
            (
            select r.network_subname, p."group", case when left(p.producer, 4) = 'XXX' then 'XXX' else p.producer end as producer, sum(r.pieces * p.weight / 1000000) as tn
            from
                retail_sales as r
                join base_sku as p on r.global_sku_code = p.global_sku_code
            where 1=1
                and r.month_year between '2024-01-01' and '2024-06-01'
                and "group" in ('group_1', 'group_2', 'group_3', 'group_4')
                and r.network_subname in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5', 'network_6', 'network_7', 'network_8', 'network_9')
            group by r.network_subname, p."group", case when left(p.producer, 4) = 'XXX' then 'XXX' else p.producer end
            ) as q1
        ) as q2
    where round((tn/nullif(tn_total, 0)*100)) > 0
    ) as curr
full join
    (
    select
        network_subname, "group", producer, tn, tn_total, round((tn/nullif(tn_total, 0)*100)) as tn_perc
    from
        (
        select network_subname, producer, "group", tn, sum(tn) over (partition by network_subname, "group") as tn_total
        from 
            (
            select r.network_subname, p."group", case when left(p.producer, 4) = 'XXX' then 'XXX' else p.producer end as producer, sum(r.pieces * p.weight / 1000000) as tn
            from
                retail_sales as r
                join base_sku as p on r.global_sku_code = p.global_sku_code
            where 1=1
                and r.month_year between '2023-01-01' and '2023-06-01'
                and "group" in ('group_1', 'group_2', 'group_3', 'group_4')
                and r.network_subname in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5', 'network_6', 'network_7', 'network_8', 'network_9')
            group by r.network_subname, p."group", case when left(p.producer, 4) = 'XXX' then 'XXX' else p.producer end
            ) as q1
        ) as q2
    where round((tn/nullif(tn_total, 0)*100)) > 0
    ) as prev
on curr.network_subname = prev.network_subname and curr."group" = prev."group" and curr.producer = prev.producer