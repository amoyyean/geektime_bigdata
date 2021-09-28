### 常见使用场景

统计各种基数(cardinality)数量庞大，对精确统计要求不高，对时效性要求较高的场景。如

- 统计截止到特定时间为止或特定时间段完成特定行为的 IP 数量；
- 统计当前访问网站或特定网页的实时客户(Unique Visitor)数量；
- 统计特定时间段内的页面浏览量(Page Views)；
- 统计用户特定时间段内搜索的不一样的关键词或词组的数量；
- 统计特定数据中心，集群或服务器上正在运行的作业(job)数量。

### 应用举例

- [Approximate Algorithms in Apache Spark: HyperLogLog and Quantiles](https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html)
- [Presto HyperLogLog Functions](https://prestodb.io/docs/current/functions/hyperloglog.html)
- [Redis HyperLogLog](https://druid.apache.org/docs/latest/querying/aggregations)
- [Druid Cardinality Aggregator](https://druid.apache.org/docs/latest/querying/aggregations)
- [DorisDB用HLL实现近似去重](http://doc.dorisdb.com/2146010)