--bai tap 1
SELECT
EXTRACT ("year" from transaction_date) as year,
product_id,
spend as curr_year_spend,
lag(spend) OVER(PARTITION BY product_id ORDER BY EXTRACT ("year" from transaction_date))
as prev_year_spend,
ROUND(((spend/lag(spend) OVER(PARTITION BY product_id ORDER BY EXTRACT ("year" from transaction_date)))-1)*100,2)
as product
FROM user_transactions order by product_id,year ASC

--bai tap 2
with first_month_sale as (
select card_name, issued_amount,
rank() over (partition by card_name order by issue_year,issue_month) 
as rank 
from monthly_cards_issued
)

select card_name,issued_amount
from first_month_sale
where rank = 1
order by issued_amount desc

--bai tap 3
with cte1 as (
SELECT user_id, spend, transaction_date,
rank () over (partition by user_id order by transaction_date) as ranking
FROM transactions)

Select user_id, spend, transaction_date
From cte1
where ranking = 3

--bai tap 4
SELECT transaction_date, user_id,
count (product_id) over (partition by user_id,transaction_date order by transaction_date)
as purchase_count
FROM user_transactions

-- bai tap 5
SELECT user_id, tweet_date,
round(avg(tweet_count) over (partition by user_id order by tweet_date), 2)
as rolling_avg_3d
FROM tweets

-- bai tap 6
--em chua lam duoc cau nay a

-- bai tap 7
with twt_total_spend as 
(SELECT category,product, 
sum(spend) as total_spend 
FROM product_spend 
where extract (year from transaction_date) = '2022'
group by category,product),
twt_ranking AS 
(select*, 
rank() over(partition by category order by total_spend desc) 
as ranking
from twt_total_spend)
select category, product, total_spend
from twt_ranking
where ranking <=2

-- bai tap 8
with cte1 as (
select a.artist_id, a.artist_name
from artists as a)

SELECT c.song_id, c.rank as artist_rank,
count (c.rank) over (partition by c.song_id order by c.rank) 
as rank_count
FROM global_song_rank as c
join songs as b
on b.song_id=c.song_id
join cte1
on cte1.artist_id=b.artist_id
--em chua lam duoc cau nay a
