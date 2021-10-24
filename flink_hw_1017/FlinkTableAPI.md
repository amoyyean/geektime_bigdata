## Flink 作业 - 实现方法public static Table report(Table transactions)

### 题目

report(transactions).executeInsert("spend_report");
将transactions表经过report函数处理后写入到spend_report表。

每分钟（小时）计算在五分钟（小时）内每个账号的平均交易金额（滑动窗口）？

注：使用分钟还是小时作为单位均可


#### 常见使用场景

HyperLogLog适合统计各种基数(cardinality)数量庞大，对精确统计要求不高，对时效性要求较高的场景。如

- 统计截止到特定时间为止或特定时间段完成特定行为的 IP 数量；
- 统计当前访问网站或特定网页的实时客户(Unique Visitor)数量；
- 统计特定时间段内的页面浏览量(Page Views)；
- 统计用户特定时间段内搜索的不一样的关键词或词组的数量；
- 统计特定数据中心，集群或服务器上正在运行的作业(job)数量。

#### 应用举例

- [Approximate Algorithms in Apache Spark: HyperLogLog and Quantiles](https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html)
- [Presto HyperLogLog Functions](https://prestodb.io/docs/current/functions/hyperloglog.html)
- [Redis HyperLogLog](https://news.ycombinator.com/item?id=7506774)
- [Druid Cardinality Estimation with Scaled HyperLogLog](https://druid.apache.org/blog/2014/02/18/hyperloglog-optimizations-for-real-world-systems.html)
- [DorisDB用HLL实现近似去重](http://doc.dorisdb.com/2146010)
- [Reddit上帖子的View Counting](https://www.redditinc.com/blog/view-counting-at-reddit/)
- [Amazon Redshift](https://aws.amazon.com/blogs/big-data/use-hyperloglog-for-trend-analysis-with-amazon-redshift/)
- [Google BigQuery](https://cloud.google.com/blog/products/gcp/counting-uniques-faster-in-bigquery-with-hyperloglog)
- [Efficient Cardinality Estimation using HLL with Spark and Postgres and Other References](https://towardsdatascience.com/efficient-cardinality-estimation-using-hll-with-spark-and-postgres-dcf1cd66ede9)
- [Counting Crowds: HyperLogLog in Simple Terms and Other References](https://medium.com/@winwardo/counting-crowds-hyperloglog-in-simple-terms-1d345637db5)
- [HyperLogLog Usage in Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/tgsql/gathering-optimizer-statistics.html#GUID-EA02EA33-9E0E-4E32-8C74-5943908D0537)

---

### 问题2: 在本地docker环境或阿里云e-mapreduce环境进行SQL查询，要求在Presto中使用HyperLogLog计算近似基数。

**建立相应表的DDL语句**：

```SQL
CREATE TABLE visit (
user_id int,
visit_date date,
media varchar
);

CREATE TABLE wp_customer_sk_approx (
hll varbinary
);
```

**生成查询数据的DML语句**：

```SQL
INSERT INTO visit VALUES 
(101, DATE('2021-09-27'), 'organic search'), 
(326, DATE('2021-10-05'), 'organic search'),
(327, DATE('2021-10-05'), 'organic search')
;

INSERT INTO wp_customer_sk_approx
SELECT cast(approx_set(wp_customer_sk) AS varbinary)
FROM web_page;
```

**获得和验证结果的DML语句**：

```SQL
SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS last3d_user_count
FROM visit_approx
WHERE visit_date >= current_date - interval '3' day;

SELECT COUNT(DISTINCT user_id) AS last3d_user_count
FROM visit
WHERE visit_date >= current_date - interval '3' day;
```

> **备注**：我们的EMR集群使用的是Presto 338。按照对应的[trino官方文档](https://trino.io/docs/338/functions/hyperloglog.html)`approx_set`函数只有1个入参，不支持表示maximum standard error的入参e。

**执行结果如下**：

EMR上HyperLogLog的查询结果为49。

![Presto_HyperLogLog_EMR_Result](Presto_HyperLogLog_EMR_Result.png)

EMR上精确的Distinct Count查询结果也为49，两者一致。

![Presto_EMR_CountDistinct_Result](Presto_EMR_CountDistinct_Result.png)

本地Docker模拟已有数据的Catalog和schema内执行INSERT语句出错，用APPROX_DISTINCT进行计算。

![Presto_EMR_CountDistinct_Result](Presto_EMR_CountDistinct_Result.png)

### 问题3: 学习使用Presto-Jdbc库连接docker或e-mapreduce环境，重复问题2的查询。（选做）

我判断解决方案是把HyperLogLog的生成和查询语句写在代码内，调用Java代码实现。如果题意理解有偏差，可以反馈。谢谢。

本地环境的实现选项1是下载Presto Github源码，本地没有Hive环境，可能还需要安装和配置。选项2是使用Docker，不确定这些Docker的Container是否可以自己Create Schema和Table。

EMR的事项可以参考[E-MapReduce>EMR开发指南>组件操作指南>Presto>使用JDBC](https://help.aliyun.com/document_detail/108859.html) 和Presto 338官方文档[Client JDBC Driver](https://trino.io/docs/338/installation/jdbc.html)。可以把‘String url = "jdbc:presto://emr-header-1:9090/hive/default";’中的相应内容换成‘jdbc:presto://106.15.194.185:9090/hive/default‘，通过Properties对象或URL传入用户名和密码。

---

## 题目

report(transactions).executeInsert("spend_report");
将transactions表经过report函数处理后写入到spend_report表。

每分钟（小时）计算在五分钟（小时）内每个账号的平均交易金额（滑动窗口）？

注：使用分钟还是小时作为单位均可

 report(transactions).executeInsert("spend_report"); 将transactions表经过report函数处理后写入到spend_report表。 每分钟（小时）计算在五分钟（小时）内每个账号的平均交易金额（滑动窗口）？ 

注：使用分钟还是小时作为单位均可

 

## 查看和验证

### Flink WebUI界面

![1](images/1.png)

![2](images/2.png)

### 实现方法report

public static Table report(Table transactions)

使用滑动窗口，每5分钟统计近10分钟内每个账号的平均交易额

```java
    public static Table report(Table transactions) {
        Timestamp time1 = new Timestamp(System.currentTimeMillis()) ;
        Table table = transactions.window(Slide.over(lit(10).minutes())
                .every(lit(5).minutes())
                .on($("transaction_time"))
                .as("log_ts"))
                .groupBy($("account_id"),$("log_ts"))
                .select($("account_id"),$("log_ts").start().as("log_ts"),$("amount").avg().as("amount"))
                ;
        return table ;
    }
```

### 实验结果

#### Mysql结果表

![3](images/3.png)

#### Grafana结果显示

![4](4.png)

