{{
    config(
        materialized='table'
    )
}}

with

source as (

    select * from {{ source('ecom', 'raw_orders') }}

    -- data runs to 2026, truncate timespan to desired range,
    -- current time as default
    where ordered_at::timestamptz <= {{ var('truncate_timespan_to') }}

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- properties
        (order_total / 100.0)::float as order_total,
        (tax_paid / 100.0)::float as tax_paid,

        ---------- timestamps
        ordered_at

    from source

)

select * from renamed
