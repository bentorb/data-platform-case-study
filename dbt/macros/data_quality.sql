{#
    Data-quality issue codes for staging models. A model selects each raw value as
    raw_<column> and its parsed value as <column>; these macros compare the two.
#}

{# '<column>_missing' when the raw value is empty, '<column>_invalid' when it could not be parsed. #}
{% macro dq_error(column) -%}
    case
        when raw_{{ column }} is null then '{{ column }}_missing'
        when {{ column }} is null then '{{ column }}_invalid'
    end
{%- endmacro %}


{# Collects issue codes into an array, leaving out the checks that found nothing. #}
{% macro dq_codes(checks) -%}
    array_remove(array[
        {%- for check in checks %}
        {{ check }}{{ "," if not loop.last }}
        {%- endfor %}
    ]::text[], null)
{%- endmacro %}
