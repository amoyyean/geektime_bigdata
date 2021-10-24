## Flink 作业 - 实现方法public static Table report(Table transactions)

### 题目

report(transactions).executeInsert("spend_report");
将transactions表经过report函数处理后写入到spend_report表。

每分钟（小时）计算在五分钟（小时）内每个账号的平均交易金额（滑动窗口）？

注：使用分钟还是小时作为单位均可


#### 相关代码

修改table-walkthrough/src/main/java/org/apache/flink/playgrounds/spendreport代码达到使用滑动窗口，每小时统计在5小时内每个账号的平均交易额。

- [修改后spendreport代码](src/SpendReport.java)

#### 执行结果和监控Dashboard

**Flink Dashboard**

Job还未开始运行时的主页面
![Flink_Dashboard_3](images/Flink_Dashboard_3.png)

Job运行时的主页面
![Flink_Dashboard_on_8082](images/Flink_Dashboard_on_8082.png)

Job页面
![Flink_Running_Job](images/Flink_Running_Job.png)

Docker修改后重新build时的主页面
![Flink_Dashboard_2](images/Flink_Dashboard_2.png)

**MySQL执行结果如下**：

mysql结果1:
![mysql_result_1](images/mysql_result_1.png)

mysql结果2:
![mysql_result_2](images/mysql_result_2.png)


**Grafana查看结果如下**：

![Grafana_Dashboard_2](images/Grafana_Dashboard_1.png)

