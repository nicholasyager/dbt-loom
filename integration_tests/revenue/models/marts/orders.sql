{{
    config(
        materialized = 'incremental',
        unique_key = 'order_id'
    )
}}

with

{#
     DuckDB will see {{ this }} evaluate to `orders` and a CTE called `orders` as being the same
     so when using DuckDB we append `_set` to any CTEs with the same name as {{ this }} to indicate
     we're not executing a recursive statement
#}

orders_set as (

    select * from {{ ref('stg_orders') }}

    where
        true

        {% if is_incremental() %}

            and ordered_at >= (
                select max(ordered_at) as most_recent_record from {{ this }}
            )

        {% endif %}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

locations as (

    select * from {{ ref('stg_locations') }}

),

supplies as (

    select * from {{ ref('stg_supplies') }}

),

order_items_summary as (

    select

        order_items.order_id,

        sum(products.is_food_item) as count_food_items,
        sum(products.is_drink_item) as count_drink_items,
        count(*) as count_items,
        sum(
            case
                when products.is_food_item = 1 then products.product_price
                else 0
            end
        ) as subtotal_drink_items,
        sum(
            case
                when products.is_drink_item = 1 then products.product_price
                else 0
            end
        ) as subtotal_food_items,
        sum(products.product_price) as subtotal

    from order_items

    left join products on order_items.product_id = products.product_id

    group by 1

),

order_supplies_summary as (

    select

        order_items.order_id,

        sum(supplies.supply_cost) as order_cost

    from order_items

    left join supplies on order_items.product_id = supplies.product_id

    group by 1

),

joined as (

    select

        orders_set.*,

        order_items_summary.count_food_items,
        order_items_summary.count_drink_items,
        order_items_summary.count_items,

        order_items_summary.subtotal_drink_items,
        order_items_summary.subtotal_food_items,
        order_items_summary.subtotal,

        order_supplies_summary.order_cost,
        locations.location_name

    from orders_set

    left join order_items_summary
        on orders_set.order_id = order_items_summary.order_id
    left join order_supplies_summary
        on orders_set.order_id = order_supplies_summary.order_id
    left join locations
        on orders_set.location_id = locations.location_id

),

final as (

    select

        *,
        count_food_items > 0 as is_food_order,
        count_drink_items > 0 as is_drink_order

    from joined

)

select * from final
