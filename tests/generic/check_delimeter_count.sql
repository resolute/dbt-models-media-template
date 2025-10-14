{% test check_delimeter_count(model, column_name, delimeter_count, delimeter = "_") %}


WITH

counts AS (

    SELECT LENGTH({{ column_name }}) - LENGTH(REGEXP_REPLACE({{ column_name }}, '{{ delimeter }}', '')) AS delimeter_count
    
    FROM {{ model }}

),

validation AS (

SELECT * FROM counts

WHERE delimeter_count != {{ delimeter_count }}

)


SELECT * FROM validation


{% endtest %}