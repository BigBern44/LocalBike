-- Dimension vendeur (staff).
-- Grain : un membre du personnel (staff_id).
-- manager_name est résolu par auto-jointure sur la même table source.

with staffs as (

    select * from {{ ref('stg_staffs') }}

),

final as (

    select
        staffs.staff_id,
        staffs.first_name,
        staffs.last_name,
        concat(staffs.first_name, ' ', staffs.last_name) as full_name,
        staffs.email,
        staffs.phone,
        staffs.active,
        staffs.store_id,
        staffs.manager_id,
        concat(manager.first_name, ' ', manager.last_name) as manager_name
    from staffs
    left join staffs as manager on staffs.manager_id = manager.staff_id

)

select * from final
