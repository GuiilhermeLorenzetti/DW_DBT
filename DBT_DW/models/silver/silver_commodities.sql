-- import 
      
with source as (
    select 
        "date",
        "Close",
        "symbol"
    from {{ source('dw_dbt', 'commodities_data') }}
),
-- renamed 
  renamed as (
    select 
        cast("date" as date) as date,
        cast("Close" as float) as close,
        cast("symbol" as text) as symbol
    from source
)
-- final table
select * from renamed


