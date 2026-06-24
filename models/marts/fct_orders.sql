-- Fait entête de commande (grain = order_id).
-- Sert l'axe Livraison (délais, retards, statut) et agrège le revenu par commande.

with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('int_order_items_enriched') }}

),

order_lines_agg as (

    select
        order_id,
        count(*)        as n_items,
        sum(quantity)   as total_quantity,
        sum(revenue)    as total_revenue
    from order_items
    group by order_id

),

final as (

    select
        -- clé (grain)
        orders.order_id,

        -- clés étrangères vers les dimensions
        orders.customer_id,
        orders.store_id,
        orders.staff_id,
        orders.order_date,            -- -> dim_date.date_day

        -- statut
        orders.order_status,
        case orders.order_status
            when 1 then 'Pending'
            when 2 then 'Processing'
            when 3 then 'Rejected'
            when 4 then 'Completed'
            else 'Unknown'
        end as order_status_label,

        -- dates de livraison
        orders.required_date,
        orders.shipped_date,

        -- mesures de délai (en jours)
        date_diff(orders.required_date, orders.order_date, day) as days_to_required,
        date_diff(orders.shipped_date,  orders.order_date, day) as shipping_days,

        -- indicateurs de livraison
        orders.shipped_date is null                            as is_not_shipped,
        coalesce(orders.shipped_date > orders.required_date, false) as is_late,

        -- mesures agrégées depuis les lignes
        coalesce(order_lines_agg.n_items, 0)        as n_items,
        coalesce(order_lines_agg.total_quantity, 0) as total_quantity,
        coalesce(order_lines_agg.total_revenue, 0)  as total_revenue

    from orders
    left join order_lines_agg on orders.order_id = order_lines_agg.order_id

)

select * from final
