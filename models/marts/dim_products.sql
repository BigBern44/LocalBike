-- Dimension produit : produits enrichis de leur marque et catégorie.
-- Grain : un produit (product_id).

with products as (

    select * from {{ ref('stg_products') }}

),

brands as (

    select * from {{ ref('stg_brands') }}

),

categories as (

    select * from {{ ref('stg_categories') }}

),

final as (

    select
        -- clé
        products.product_id,

        -- attributs produit
        products.product_name,
        products.model_year,
        products.list_price,

        -- marque
        products.brand_id,
        brands.brand_name,

        -- catégorie
        products.category_id,
        categories.category_name

    from products
    left join brands     on products.brand_id    = brands.brand_id
    left join categories on products.category_id = categories.category_id

)

select * from final
