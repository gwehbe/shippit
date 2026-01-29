{{ config(materialized='view') }}

with source as (
    select
        try_cast(event_id as text) as event_id,
        try_cast(event_type as text) as event_type,
        try_cast(order_id as text) as order_id,
        try_cast(shipment_id as text) as shipment_id,
        try_cast(event_timestamp as timestamp) as event_timestamp,
        try_cast(ingested_at as timestamp) as ingested_at,
        try_cast(carrier as text) as carrier,
        try_cast(attributes as json) as event_attributes
    from {{ ref('shipment_lifecycle_events') }}
)

select * from source
