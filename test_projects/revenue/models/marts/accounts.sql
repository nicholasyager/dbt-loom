with

final as (
    select name from {{ ref('stg_accounts') }}
)


select * from final
