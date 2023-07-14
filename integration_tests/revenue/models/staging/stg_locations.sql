
with

source as (

    select * from {{ source('ecom', 'raw_stores') }}

    {# data runs to 2026, truncate timespan to desired range, 
    current time as default #}
    where opened_at::timestamptz <= {{ var('truncate_timespan_to') }}

),

renamed as (

    select

        ----------  ids
        id as location_id,

        ---------- properties
        name as location_name,
        tax_rate,

        ---------- timestamp
        opened_at

    from source

)

select * from renamed
