-- Test singulier (cohérence inter-faits) : le revenu agrégé dans `fct_orders`
-- (total_revenue, grain commande) doit égaler la somme des revenus des lignes
-- correspondantes dans `fct_order_items` (grain ligne).
-- Tolérance de 0.01 pour les arrondis numériques. Doit renvoyer 0 ligne.

with lines_agg as (

    select
        order_id,
        sum(revenue) as revenue_from_lines
    from {{ ref('fct_order_items') }}
    group by order_id

),

orders as (

    select
        order_id,
        total_revenue
    from {{ ref('fct_orders') }}

)

select
    orders.order_id,
    orders.total_revenue,
    lines_agg.revenue_from_lines
from orders
join lines_agg using (order_id)
where abs(orders.total_revenue - lines_agg.revenue_from_lines) > 0.01
