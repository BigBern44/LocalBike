-- Grain : une ligne de commande (order_id, item_id).
-- Enrichit order_items avec produit / marque / catégorie et calcule le revenu.
--   revenue = quantity * list_price * (1 - discount)   (prix de la ligne de commande)

with order_items as (

    select * from {{ ref('stg_order_items') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

brands as (

    select * from {{ ref('stg_brands') }}

),

categories as (

    select * from {{ ref('stg_categories') }}

),

enriched as (

    select
        -- clés
        order_items.order_id,
        order_items.item_id,
        order_items.product_id,
        products.brand_id,
        products.category_id,

        -- attributs produit
        products.product_name,
        products.model_year,
        brands.brand_name,
        categories.category_name,

        -- mesures
        order_items.quantity,
        order_items.list_price,
        order_items.discount,
        round(order_items.quantity * order_items.list_price * (1 - order_items.discount), 2) as revenue

    from order_items
    left join products   on order_items.product_id = products.product_id
    left join brands     on products.brand_id      = brands.brand_id
    left join categories on products.category_id   = categories.category_id

)

select * from enriched
