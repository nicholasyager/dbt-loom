-- Tests cross-project event_time configurations to support microbatching
-- materialization between different dbt projects
{{
    config(
        materialized='incremental',
        incremental_strategy="microbatch",
        begin='2016-01-01',
        event_time="ordered_at_date",
        batch_size='day',
        enabled=var('test_microbatch_event_time', False)
    )
}}
select
    order_id,
    customer_id
from {{ ref('revenue', 'orders') }}
