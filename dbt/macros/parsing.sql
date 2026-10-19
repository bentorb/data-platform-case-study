{#
    Safe parsing of raw text values. Each macro returns the typed value when the text
    matches an accepted format, and null otherwise, so a bad value can never make a
    query fail. Used with dq_error() to flag the values that could not be parsed.
#}

{#
    Integers, optionally written as N.0: tools such as pandas export integer columns
    that contain nulls as floats. A known prefix (e.g. 'T' in 'T1010') is stripped.
    At most 9 digits, so the value always fits an integer.
#}
{% macro parse_integer(column, prefix=none) -%}
    {%- set prefix_pattern = '(' ~ prefix ~ ')?' if prefix else '' -%}
    {%- set digits = "regexp_replace(" ~ column ~ ", '^" ~ prefix ~ "', '')" if prefix else column -%}
    case
        when {{ column }} ~ '^{{ prefix_pattern }}[0-9]{1,9}([.]0+)?$'
            then {{ digits }}::numeric::integer
    end
{%- endmacro %}


{#
    Decimals such as 200.35 or -3, as fixed-point numeric (exact, unlike float). The
    cast wraps the whole case: inside a branch, Postgres would drop the precision.
#}
{% macro parse_decimal(column, precision=12, scale=2) -%}
    (case
        when {{ column }} ~ '^-?[0-9]{1,{{ precision - scale }}}([.][0-9]+)?$'
            then {{ column }}::numeric
    end)::numeric({{ precision }}, {{ scale }})
{%- endmacro %}


{#
    Dates as YYYY-MM-DD or DD-MM-YYYY. The pattern checks month and day ranges; an
    impossible date that passes it (e.g. 2023-02-30) still fails the query, loudly.
#}
{% macro parse_date(column) -%}
    case
        when {{ column }} ~ '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'
            then to_date({{ column }}, 'YYYY-MM-DD')
        when {{ column }} ~ '^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-[0-9]{4}$'
            then to_date({{ column }}, 'DD-MM-YYYY')
    end
{%- endmacro %}
