-- Reporting plat (dénormalisé) pour Looker Studio — axe Stocks.
-- Grain : un couple magasin/produit (store_id + product_id).
-- Aplatit fct_stocks + dimensions en une table large, lisible directement par
-- le dashboard. Sert l'axe Stocks (niveau par magasin, ruptures, valorisation).

with stocks as (

    select * from {{ ref('fct_stocks') }}

),

stores as (

    select * from {{ ref('dim_stores') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

final as (

    select
        -- clé (grain)
        stocks.stock_key,

        -- magasin
        stocks.store_id,
        stores.store_name,
        stores.city  as store_city,
        stores.state as store_state,

        -- produit / marque / catégorie
        stocks.product_id,
        products.product_name,
        products.brand_name,
        products.category_name,
        products.model_year,
        products.list_price,

        -- mesures
        stocks.quantity,
        stocks.stock_value,
        stocks.is_out_of_stock

    from stocks
    left join stores   on stocks.store_id   = stores.store_id
    left join products on stocks.product_id = products.product_id

)

select * from final
