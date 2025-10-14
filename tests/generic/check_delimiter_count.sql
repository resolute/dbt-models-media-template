{% test check_delimiter_count(model, column_name, delimiter_count, delimiter = "_") %}


WITH

counts AS (

    SELECT LENGTH({{ column_name }}) - LENGTH(REGEXP_REPLACE({{ column_name }}, '{{ delimiter }}', '')) AS delimiter_count
    
    FROM {{ model }}

),

validation AS (

SELECT * FROM counts

WHERE delimiter_count != {{ delimiter_count }}

)


SELECT * FROM validation


{% endtest %}