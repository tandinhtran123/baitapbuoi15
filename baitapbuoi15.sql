--bai 1
SELECT
EXTRACT(year from transaction_date) as year,
product_id,
spend as curr_year_spend,
lag(spend) OVER(PARTITION BY product_id ORDER BY EXTRACT(year from transaction_date)) as prev_year_spend,
ROUND(((spend/lag(spend) OVER(PARTITION BY product_id ORDER BY EXTRACT ("year" from transaction_date)))-1)*100,2) as yoy_rate
FROM user_transactions
ORDER BY product_id, year
--bai 2
with cte AS
(SELECT 
card_name,
issued_amount,
rank() OVER(PARTITION BY card_name order by issue_year, issue_month) as rank
FROM monthly_cards_issued)
select card_name, issued_amount
from cte where rank=1
--bai 3
with cte as (SELECT user_id, spend, transaction_date,
rank() OVER(PARTITION BY user_id ORDER BY transaction_date) as rank
FROM transactions)
select user_id, spend, transaction_date from cte 
where rank=3
--bai 4
SELECT distinct transaction_date,user_id, purchase_count
FROM
(SELECT transaction_date, user_id,
count(*) OVER (PARTITION BY user_id order by transaction_date desc) as purchase_count,
rank() OVER(PARTITION BY user_id order by transaction_date desc) as most_recent_date
FROM user_transactions) as count_transaction
where most_recent_date=1
--bai 5
select user_id, tweet_date,
ROUND(AVG(tweet_count) OVER (PARTITION BY user_id ORDER BY tweet_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2) as rolling_avg_3d
from tweets
--bai 6
with cte1 AS(SELECT *,
lead(transaction_timestamp)
over(PARTITION BY merchant_id,merchant_id, credit_card_id,
amount order by transaction_timestamp) as next_transaction_timestamp
from transactions)
Select Count(*) as payment_count 
FROM
(select * from cte1
where next_transaction_timestamp is not null) as transactionss
Where 
EXTRACT (EPOCH from next_transaction_timestamp-transaction_timestamp)/60 <=10
--bai 7
select category, product, total_spend 
from (select category,product, 
sum(spend) as total_spend,
rank () over (partition by category order by sum(spend) desc) as ranking
from product_spend
where extract (year from transaction_date)= '2022'
group by category,product) category_ranking
where ranking in ('1','2')
--bai 8
with cte as 
(SELECT a.artist_name, b.name as song_name, c.rank
FROM artists a JOIN songs b on a.artist_id = b.artist_id
JOIN global_song_rank c on b.song_id =c.song_id),

ct2 AS
(select artist_name,
sum(case when rank between 1 and 10 then 1 else 0 END) as top_count
from cte
group by artist_name)

SELECT artist_name,
dense_rank() over (order by top_count desc) as artist_rank
from ct2
limit 5
