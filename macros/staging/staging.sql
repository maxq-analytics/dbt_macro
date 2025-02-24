-- Macro for selecting most recent ID based on extraction column
-- filter_id_latest
-- Extract only on latest ID if there are duplicates for the same ID

{% macro id_on_latest(id, extraction, table) %}
    select {{ id }} as unique_id, max({{ extraction }}) as latest_extraction_at
    from {{ table }}
    group by {{ id }}
{% endmacro %}

{% macro filter_id_latest(id, extraction, table) %}
    inner join
        ({{ id_on_latest(id, extraction, table) }}) as latest_extraction
        on latest_extraction.latest_extraction_at = {{ table }}.{{ extraction }}
        and latest_extraction.unique_id = {{ table }}.{{ id }}

    order by {{ table }}.{{ id }} desc

{% endmacro %}