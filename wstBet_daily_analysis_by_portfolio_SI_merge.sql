DECLARE @date_threshold AS DATETIME2
SET @date_threshold = DATEADD(DAY, -7, GETDATE());

WITH weekly_analysis_portfolio (stock_match_date, Position, Analyst, ticker, post_count, WoW_post_count_change, put_count, WoW_put_count_change, call_count, WoW_call_count_change, pump_count, dump_count) AS (
select  CAST(GETDATE() AS DATE) AS stock_match_date, Position, Analyst, ticker, post_count, WoW_post_count_change, put_count, WoW_put_count_change, call_count, WoW_call_count_change, pump_count, dump_count
from(
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
 where stock_match_date >= @date_threshold and stock_match_date NOT IN ('2020-09-16 00:00:00.0000000') AND ticker not in ('VXX', 'USA', 'YOLO', 'IPO', 'EV', 'EOD', 'SPY', 'QQQ', 'TQQQ', 'SQQQ', 'IWM', 'HYG', 'GLD', 'LQD', 'TLT', 'XLU', 'XLE')
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
 ) past_week on current_week.ticker = past_week.ticker) weekly_perf inner join dbo.daily_portfolio_report on daily_portfolio_report.SecurityID = weekly_perf.ticker 
 where weekly_perf.post_count >=5)

 /* use union all to achieve segmentaton and sort within each segmentation */
 select  Position, Analyst, ticker, recent_SI.Sedol, recent_SI.Description, post_count, WoW_post_count_change,put_count, WoW_put_count_change, call_count, WoW_call_count_change,pump_count, dump_count, recent_SI.[Utilization Band], recent_SI.[Demand 1D % ∆], recent_SI.[Demand 1W % ∆], recent_SI.[Demand 1M % ∆], recent_SI.[Demand 3M % ∆], recent_SI.[Demand YTD % ∆] 
 from (
 select *
 from ( select top 100 *
		from weekly_analysis_portfolio
		where Position = 'LONG'
		order by  post_count desc, put_count desc, call_count desc) long

UNION ALL
select *
 from ( select top 100 *
		from weekly_analysis_portfolio
		where Position = 'SHORT'
		order by  post_count desc, put_count desc, call_count desc) short
UNION ALL
select *
 from ( select top 100 *
		from weekly_analysis_portfolio
		where Position = 'PIPELINE'
		order by  post_count desc, put_count desc, call_count desc) pipeline
UNION ALL
select *
 from ( select top 100 *
		from weekly_analysis_portfolio
		where Position = 'PASS'
		order by  post_count desc, put_count desc, call_count desc) pass) book left outer join (select BBG, Sedol, Description, [Utilization Band], [Demand 1D % ∆], [Demand 1W % ∆], [Demand 1M % ∆], [Demand 3M % ∆], [Demand YTD % ∆] from WSTBET.SI) recent_SI on 
book.ticker = recent_SI.BBG



