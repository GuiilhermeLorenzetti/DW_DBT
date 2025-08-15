--impor 

 with source as (
    select 
        "date",
        "symbol",
        "action",
        "quantity"
    from {{ source('dw_dbt', 'trades') }}
),
-- renamed
  renamed as (
    select 
        cast("date" as date) as date,
        cast("symbol" as text) as symbol,
        cast("action" as text) as action,
        cast("quantity" as int) as quantity
    from source
)
select * from renamed









