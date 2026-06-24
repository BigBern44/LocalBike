-- Reporting plat (dénormalisé) pour Looker Studio — axe Livraison / Commandes.
-- Grain : une commande (order_id).
-- Aplatit fct_orders + dimensions en une table large, lisible directement par
-- le dashboard. Sert l'axe Livraison (délais, retards, statut) et le revenu/commande.

with orders as (

    select * from {{ ref('fct_orders') }}

),

stores as (

    select * from {{ ref('dim_stores') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

),

staffs as (

    select * from {{ ref('dim_staffs') }}

),

dates as (

    select * from {{ ref('dim_date') }}

),

final as (

    select
        -- clé (grain)
        orders.order_id,

        -- temps (depuis dim_date)
        orders.order_date,
        dates.year       as order_year,
        dates.quarter    as order_quarter,
        dates.month      as order_month,
        dates.year_month as order_year_month,

        -- dates de livraison
        orders.required_date,
        orders.shipped_date,

        -- statut
        orders.order_status,
        orders.order_status_label,

        -- délais & indicateurs de livraison
        orders.days_to_required,
        orders.shipping_days,
        orders.is_not_shipped,
        orders.is_late,

        -- magasin
        orders.store_id,
        stores.store_name,
        stores.city  as store_city,
        stores.state as store_state,

        -- client
        orders.customer_id,
        customers.full_name as customer_name,
        customers.city      as customer_city,
        customers.state     as customer_state,

        -- vendeur
        orders.staff_id,
        staffs.full_name as staff_name,

        -- mesures
        orders.n_items,
        orders.total_quantity,
        orders.total_revenue

    from orders
    left join stores    on orders.store_id    = stores.store_id
    left join customers on orders.customer_id = customers.customer_id
    left join staffs    on orders.staff_id    = staffs.staff_id
    left join dates     on orders.order_date  = dates.date_day

)

select * from final
