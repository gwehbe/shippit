{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_id',
    on_schema_change='append_new_columns'    
) }}

with raw_events as (
    select
        *
    from {{ ref('stg_shipment_events') }}
    {% if is_incremental() %}
        where ingested_at >= (select max(ingested_at) - interval 3 day from {{ this }})
    {% endif %}
),

deduped_events as (
    select
        *,
        row_number() over (
            partition by event_id
            order by event_timestamp desc, ingested_at desc
        ) as row_num
    from raw_events
),

final as (
    select
        event_id,
        event_type,
        order_id,
        shipment_id,
        event_timestamp,
        ingested_at,
        carrier,
        event_attributes
    from deduped_events
    where row_num = 1
)

select * from final
