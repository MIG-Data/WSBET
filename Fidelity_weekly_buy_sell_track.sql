DECLARE @date_threshold AS DATETIME2
SET @date_threshold = DATEADD(DAY, -7, GETDATE());

select curr_week.symbol, CAST(curr_week.average_rank AS INT) as average_rank, ROUND((curr_week.average_rank - prev_week.average_rank ) / NULLIF(prev_week.average_rank, 0),2)  as WoW_rank_change, 
curr_week.total_weekly_buy, ROUND((CAST(curr_week.total_weekly_buy AS FLOAT) - prev_week.total_weekly_buy ) / NULLIF(prev_week.total_weekly_buy, 0),2)  as WoW_buy_change, 
curr_week.total_weekly_sell, ROUND((CAST(curr_week.total_weekly_sell AS FLOAT) - prev_week.total_weekly_sell ) / NULLIF(prev_week.total_weekly_sell, 0),2)  as WoW_sell_change, 
curr_week.total_weekly_order, ROUND((CAST(curr_week.total_weekly_order AS FLOAT) - prev_week.total_weekly_order ) / NULLIF(prev_week.total_weekly_order, 0),2)  as WoW_total_order_change,
curr_week.buy_sell_ratio, ROUND((curr_week.buy_sell_ratio - prev_week.buy_sell_ratio ) / NULLIF(prev_week.buy_sell_ratio, 0),2)  as WoW_buy_sell_ratio_change
from (
select  symbol, AVG(CAST (rank as FLOAT)) as average_rank, SUM(buy_order_quantity) as total_weekly_buy , SUM(sell_order_quantity) as total_weekly_sell, (SUM(buy_order_quantity) + SUM(sell_order_quantity)) as total_weekly_order, ROUND((CAST(SUM(buy_order_quantity)AS FLOAT) / SUM(sell_order_quantity)),2) as buy_sell_ratio
from [WSTBET].[Fidelity_buy_sell]
 where  @date_threshold < process_datetime and symbol NOT IN ('VXX', 'USA', 'YOLO', 'IPO', 'EV', 'EOD', 'SPY', 'QQQ', 'TQQQ', 'SQQQ', 'IWM', 'HYG', 'GLD', 'LQD', 'TLT', 'XLU', 'XLE')
 group by symbol) curr_week left outer join
 (select symbol, AVG(CAST (rank as FLOAT)) as average_rank, SUM(buy_order_quantity) as total_weekly_buy , SUM(sell_order_quantity) as total_weekly_sell, (SUM(buy_order_quantity) + SUM(sell_order_quantity)) as total_weekly_order, ROUND((CAST(SUM(buy_order_quantity) AS FLOAT) / SUM(sell_order_quantity)),2) as buy_sell_ratio
from [WSTBET].[Fidelity_buy_sell]
 where  DATEADD(DAY, -7, @date_threshold)<=process_datetime AND process_datetime<@date_threshold
 group by symbol) prev_week on curr_week.symbol = prev_week.symbol
 order by average_rank asc
