{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    on_schema_change='append_new_columns'    
) }}

with events as (
    select
        *
    from {{ ref('int_shipment_events') }}
    {% if is_incremental() %}
        where ingested_at >= (select max(last_updated_at) - interval 3 day from {{ this }})
    {% endif %}
),

orders as (
    select
        order_id,
        min(event_timestamp) as first_event_time,
        max(event_timestamp) as latest_event_time,
        arg_max(event_type, event_timestamp) as current_shipment_status,
        arg_max(carrier, event_timestamp) as carrier,
        max(ingested_at) as last_updated_at
    from events
    group by order_id
)

select * from orders
