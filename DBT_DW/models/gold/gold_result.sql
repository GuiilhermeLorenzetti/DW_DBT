with source_commodities as (
    select* from {{ ref('silver_commodities')}}
),
source_trades as (
    select* from {{ ref('silver_trade')}}
),
final as (
    select 
        source_commodities.date,
        source_commodities.symbol,
        source_commodities.close,
        source_trades.action,
        source_trades.quantity
    from source_commodities
    left join source_trades
    on source_commodities.date = source_trades.date
    and source_commodities.symbol = source_trades.symbol
)
select * from final




