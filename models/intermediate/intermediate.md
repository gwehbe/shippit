{% docs int_shipment_events %}
Deduplicated version of `stg_shipment_events`.
{% enddocs %}

{% docs first_event_time %}
The timestamp of the first shipment lifecycle event for the order.
{% enddocs %}

{% docs latest_event_time %}
The timestamp of the latest shipment lifecycle event for the order.
{% enddocs %}

{% docs current_shipment_status %}
The latest `event_type` for the order.
{% enddocs %}

{% docs last_updated_at %}
The timestamp of when the latest event timestamp was ingested for the order.
{% enddocs %}
