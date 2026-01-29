{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    on_schema_change='append_new_columns'    
) }}

with orders as (
    select * from {{ ref('int_orders') }}
    {% if is_incremental() %}
        where last_updated_at >= (select max(last_updated_at) - interval 3 day from {{ this }})
    {% endif %}
),

final as (
    select
        order_id,
        first_event_time,
        latest_event_time,
        current_shipment_status,
        carrier,
        last_updated_at
    from orders
    order by last_updated_at desc
)

select * from final
