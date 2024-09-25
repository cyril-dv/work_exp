select 
    case 
        when month_year between '2024-04-01' and '2024-06-01' then 'prior_3m_1'
        when month_year between '2024-01-01' and '2024-03-01' then 'prior_3m_2'
    end as month_year,
    network_subname,
    "group",
    producer,
    avg(tn) as tn
from (
    select 
        r.month_year,
        case when r.network_subname in ('____') then 'network_1' else r.network_subname end as network_subname, 
        p."group", 
        case 
            when (p."group" = 'group_1' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5', 'producer_6', 'producer_7')) then p.producer 
            when (p."group" = 'group_2' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5', 'producer_6', 'producer_7', 'producer_8', 'producer_9'))  then p.producer
            when (p."group" = 'group_3' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4')) then p.producer
            when (p."group" = 'group_4' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5', 'producer_6', 'producer_7', 'producer_8', 'producer_9')) then p.producer  
            when (p."group" = 'group_5' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5', 'producer_6', 'producer_7', 'producer_8', 'producer_9')) then p.producer
            when (p."group" = 'group_6' and producer in ('producer_1', 'producer_2', 'producer_3')) then p.producer
            when producer in ('producer_A', 'producer_B', 'producer_C', 'producer_D', 'producer_E') then 'XXX'
            else 'Прочие'
        end as producer,  
        sum(r.pieces * p.weight / 1000000) as tn
    from
        retail_sales as r
        join base_sku as p on r.global_sku_code = p.global_sku_code
    where 1=1
        and r.month_year between '2024-01-01' and '2024-06-01'
        and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6')
        and r.network_subname in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5', 'network_6', 'network_7', 'network_8', 'network_9', 'network_10')
    group by 
        r.month_year,
        case when r.network_subname in ('____') then 'network_1' else r.network_subname end as network_subname, 
        p."group", 
        case 
            when (p."group" = 'group_1' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5', 'producer_6', 'producer_7')) then p.producer 
            when (p."group" = 'group_2' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5', 'producer_6', 'producer_7', 'producer_8', 'producer_9'))  then p.producer
            when (p."group" = 'group_3' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4')) then p.producer
            when (p."group" = 'group_4' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5', 'producer_6', 'producer_7', 'producer_8', 'producer_9')) then p.producer  
            when (p."group" = 'group_5' and producer in ('producer_1', 'producer_2', 'producer_3', 'producer_4', 'producer_5', 'producer_6', 'producer_7', 'producer_8', 'producer_9')) then p.producer
            when (p."group" = 'group_6' and producer in ('producer_1', 'producer_2', 'producer_3')) then p.producer
            when producer in ('producer_A', 'producer_B', 'producer_C', 'producer_D', 'producer_E') then 'XXX'
            else 'Прочие' 
        end
) as q
group by
    case 
        when month_year between '2024-04-01' and '2024-06-01' then 'prior_3m_1'
        when month_year between '2024-01-01' and '2024-03-01' then 'prior_3m_2'
    end,
    network_subname,
    "group",
    producer
order by 
    network_subname, 
    "group", 
    producer, 
    case 
        when month_year between '2024-04-01' and '2024-06-01' then 'prior_3m_1'
        when month_year between '2024-01-01' and '2024-03-01' then 'prior_3m_2'
    end