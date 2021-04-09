DECLARE @date_threshold AS DATETIME2
SET @date_threshold = DATEADD(DAY, -7, GETDATE());

select daily_post.ticker, recent_SI.Sedol, recent_SI.Description, daily_post.post_count, daily_post.WoW_post_count_change,daily_post.put_count, daily_post.WoW_put_count_change, daily_post.call_count, daily_post.WoW_call_count_change,daily_post.pump_count, daily_post.dump_count, recent_SI.[Utilization Band], recent_SI.[Demand 1D % ∆], recent_SI.[Demand 1W % ∆], recent_SI.[Demand 1M % ∆], recent_SI.[Demand 3M % ∆], recent_SI.[Demand YTD % ∆]
from (
select current_week.ticker, current_week.post_count,   ROUND((CAST(current_week.post_count as FLOAT) - cast(past_week.post_count as FLOAT)) / NULLIF(cast(past_week.post_count as FLOAT), 0),2)  as WoW_post_count_change,  
current_week.put_count, ROUND((CAST(current_week.put_count as FLOAT) - cast(past_week.put_count as FLOAT)) / NULLIF(cast(past_week.put_count as FLOAT), 0),2)  as WoW_put_count_change, 
current_week.call_count, ROUND((CAST(current_week.call_count as FLOAT)- cast(past_week.call_count as FLOAT)) / NULLIF(cast(past_week.call_count as FLOAT), 0),2)  as WoW_call_count_change, 
current_week.pump_count, 
current_week.dump_count
from (
SELECT 
      [ticker],
      SUM(CAST([post_count] AS INT)) as post_count,
      SUM(CAST([author_count] AS INT)) as author_count,
      
      SUM(CAST([put] AS INT)) as put_count,
      SUM(CAST([call] AS INT)) as call_count,
      SUM(CAST([pump] AS INT)) as pump_count,
      SUM(CAST([dump] AS INT)) as dump_count
  FROM [wstBet].[reddit]
 where stock_match_date >= @date_threshold and stock_match_date NOT IN ('2020-09-16 00:00:00.0000000')
  GROUP BY  ticker) current_week left outer join
(
SELECT 
      [ticker],
      SUM(CAST([post_count] AS INT)) as post_count,
      SUM(CAST([author_count] AS INT)) as author_count,
      
      SUM(CAST([put] AS INT)) as put_count,
      SUM(CAST([call] AS INT)) as call_count,
      SUM(CAST([pump] AS INT)) as pump_count,
      SUM(CAST([dump] AS INT)) as dump_count
  FROM [wstBet].[reddit]
 where DATEADD(DAY, -7, @date_threshold)<=stock_match_date AND stock_match_date <@date_threshold
  GROUP BY  ticker
 ) past_week on current_week.ticker = past_week.ticker
 where current_week.post_count >10 AND current_week.ticker not in ('MOON', 'UI', 'VXX', 'USA', 'YOLO', 'IPO', 'EV', 'EOD', 'SPY', 'QQQ', 'TQQQ', 'SQQQ', 'IWM', 'HYG', 'GLD', 'LQD', 'TLT', 'XLU', 'XLE', 'DD', 'YOLO', 'HOLD')
) daily_post left outer join (select BBG, Sedol, Description, [Utilization Band], [Demand 1D % ∆], [Demand 1W % ∆], [Demand 1M % ∆], [Demand 3M % ∆], [Demand YTD % ∆] from WSTBET.SI) recent_SI on 
daily_post.ticker = recent_SI.BBG
order by daily_post.post_count desc, daily_post.put_count desc, daily_post.call_count desc