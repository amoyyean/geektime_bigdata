select t1.age
,avg(t2.rate) as avg_rating
from
t_user t1 join t_rating t2
on
(t1.userid = t2.userid)
where t2.movieid = '2116'
group by t1.age;