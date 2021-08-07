/* 方法1 - 分3步 */

/* 1. 找出评分最多的女性的userid为1150 */

select t2.userid, count(t2.userid) as total
from
t_user t1 join t_rating t2
on
(t1.userid = t2.userid)
where
t1.sex = 'F'
group by t2.userid
order by total desc
limit 1;

/* 2. 找出userid为1150的女性评分最高的10部电影(由于5分制,最高评分的应该不止10部)的movieid并保存到临时表中 */

create table top10_of_1150_ly as
select movieid, rate
from t_rating
where
userid = 1150
order by rate desc
limit 10;

/* 第2步的另1种方法 */

create table top10_of_1150 as
	select * from (
		select movieid, rate, row_number() over (order by rate desc) rate_rank
		from t_rating
		where
		userid = 1150) t1
	where
	rate_rank <= 10;

/* 3. 从t_rating表里找出上面选出的10个movieid的平均影评分 */

select t3.movieid, t3.moviename, avg(t2.rate) as avgrate
from
top10_of_1150_ly t1 join t_rating t2
on
(t1.movieid = t2.movieid)
join t_movie t3
on
(t2.movieid = t3.movieid)
group by
t3.movieid, t3.moviename
order by t3.movieid;

/* 方法2 */

select t3.movieid, t3.moviename
,avg(t2.rate) as avg_rating
from
(select t2_2.movieid, t2_2.rate
from
(select t2.userid, count(t2.userid) as total
from
t_user t1 join t_rating t2
on
(t1.userid = t2.userid)
where
t1.sex = 'F'
group by t2.userid
order by total desc
limit 1) t2_1
join t_rating t2_2
on
(t2_1.userid = t2_2.userid)
order by t2_2.rate desc, t2_2.movieid
limit 10) t3_1
join t_movie t3
on
(t3_1.movieid = t3.movieid)
join t_rating t2
on
(t3_1.movieid = t2.movieid)
group by t3.movieid, t3.moviename
order by t3.movieid;
