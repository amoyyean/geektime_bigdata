select t1.sex
,t3.moviename as name
,avg(t2.rate) as avg_rating
,count(t2.userid) as total
from
t_user t1 join t_rating t2
on
(t1.userid = t2.userid)
join t_movie t3
on
(t2.movieid = t3.movieid)
where
t1.sex = 'M'
group by t1.sex, t3.moviename
having total > 50
order by avg_rating desc
limit 10;