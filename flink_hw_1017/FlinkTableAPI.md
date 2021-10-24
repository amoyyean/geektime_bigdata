## Flink 作业 - 实现方法public static Table report(Table transactions)

### 题目

report(transactions).executeInsert("spend_report");
将transactions表经过report函数处理后写入到spend_report表。

每分钟（小时）计算在五分钟（小时）内每个账号的平均交易金额（滑动窗口）？

注：使用分钟还是小时作为单位均可


#### 相关代码

修改table-walkthrough/src/main/java/org/apache/flink/playgrounds/spendreport代码达到使用滑动窗口，每小时统计在5小时内每个账号的平均交易额。

- [修改后spendreport代码](src/SpendReport.java)

#### Flink Dashboard

Job还未开始运行时的主页面
![Flink_Dashboard_3](imgs/Flink_Dashboard_3.png)

Job运行时的主页面
![Flink_Dashboard_on_8082](imgs/Flink_Dashboard_on_8082.png)

Job页面
![Flink_Running_Job](imgs/Flink_Running_Job.png)

Docker修改后重新build时的主页面
![Flink_Dashboard_2](imgs/Flink_Dashboard_2.png)

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

> **备注**：我们的EMR集群使用的是Presto 338。按照对应的[trino官方文档](https://trino.io/docs/338/functions/hyperloglog.html)`approx_set`函数只有1个入参，不支持表示maximum standard error的入参e。

**MySQL执行结果如下**：

mysql结果1:
![mysql_result_1](imgs/mysql_result_1.png)

mysql结果2:
![mysql_result_2](imgs/mysql_result_2.png)


**Grafana查看结果如下**：

![Grafana_Dashboard_2](imgs/Grafana_Dashboard_1.png)

---

## 题目


 report(transactions).executeInsert("spend_report"); 将transactions表经过report函数处理后写入到spend_report表。 每分钟（小时）计算在五分钟（小时）内每个账号的平均交易金额（滑动窗口）？ 

注：使用分钟还是小时作为单位均可

