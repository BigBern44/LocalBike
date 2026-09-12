-- Schéma source BikeStores (9 tables) pour Supabase / PostgreSQL.
--
-- Recrée à l'identique le modèle décrit dans CLAUDE.md §5 : les 9 tables vivent
-- toutes dans le schéma `public`, regroupées logiquement en un domaine *ventes*
-- (customers, orders, order_items, staffs, stores) et un domaine *production*
-- (categories, products, stocks, brands).
--
-- Appliqué par `ingestion/load_parquet_to_supabase.py` avant le chargement des
-- fichiers Parquet de `data_raw/`. Idempotent : `create table if not exists`.
--
-- Les types restent ceux de BikeStores (clés entières, dates natives, prix en
-- numeric) : les modèles staging dbt les castent explicitement côté BigQuery.

-- --------------------------------------------------------------------------
-- Domaine production
-- --------------------------------------------------------------------------

create table if not exists public.brands (
    brand_id   integer      primary key,
    brand_name varchar(255) not null
);

create table if not exists public.categories (
    category_id   integer      primary key,
    category_name varchar(255) not null
);

create table if not exists public.products (
    product_id   integer       primary key,
    product_name varchar(255)  not null,
    brand_id     integer       not null references public.brands (brand_id),
    category_id  integer       not null references public.categories (category_id),
    model_year   smallint      not null,
    list_price   numeric(10,2) not null
);

-- --------------------------------------------------------------------------
-- Domaine ventes
-- --------------------------------------------------------------------------

create table if not exists public.stores (
    store_id   integer      primary key,
    store_name varchar(255) not null,
    phone      varchar(25),
    email      varchar(255),
    street     varchar(255),
    city       varchar(255),
    state      varchar(10),
    zip_code   varchar(5)
);

-- `manager_id` s'auto-référence (hiérarchie des vendeurs) : la contrainte est
-- ajoutée après le chargement, l'ordre des lignes du Parquet n'étant pas garanti
-- topologique (cf. add_self_reference dans le script de chargement).
create table if not exists public.staffs (
    staff_id   integer      primary key,
    first_name varchar(50)  not null,
    last_name  varchar(50)  not null,
    email      varchar(255) not null unique,
    phone      varchar(25),
    active     smallint     not null,
    store_id   integer      not null references public.stores (store_id),
    manager_id integer
);

create table if not exists public.customers (
    customer_id integer      primary key,
    first_name  varchar(255) not null,
    last_name   varchar(255) not null,
    phone       varchar(25),
    email       varchar(255) not null,
    street      varchar(255),
    city        varchar(50),
    state       varchar(25),
    zip_code    varchar(5)
);

create table if not exists public.orders (
    order_id      integer  primary key,
    customer_id   integer  references public.customers (customer_id),
    order_status  smallint not null,
    order_date    date     not null,
    required_date date     not null,
    shipped_date  date,
    store_id      integer  not null references public.stores (store_id),
    staff_id      integer  not null references public.staffs (staff_id)
);

-- Grain de la table de faits principale : une ligne = un produit d'une commande.
create table if not exists public.order_items (
    order_id   integer       not null references public.orders (order_id),
    item_id    integer       not null,
    product_id integer       not null references public.products (product_id),
    quantity   integer       not null,
    list_price numeric(10,2) not null,
    discount   numeric(4,2)  not null default 0,
    primary key (order_id, item_id)
);

-- Snapshot d'inventaire : quantité par (magasin, produit).
create table if not exists public.stocks (
    store_id   integer not null references public.stores (store_id),
    product_id integer not null references public.products (product_id),
    quantity   integer,
    primary key (store_id, product_id)
);
