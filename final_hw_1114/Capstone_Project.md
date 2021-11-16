# 作业3 Insert命令自动合并小文件

- 我们讲过AQE可以自动调整reducer的个数，但是正常跑Insert命令不会自动合并小文件，例如
```sql
insert into t1 select * from t2;
  ```
- 请加一条物理规则（Strategy），让Insert命令自动进行小文件合并(repartition)。（不用考虑bucket表，不用考虑Hive表）

## 代码参考

```scala
object RepartitionForInsertion extends Rule[SparkPlan] {
override def apply(plan: SparkPlan): SparkPlan = {
plan transformDown {
case i @ InsertIntoDataSourceExec(child, _, _, partitionColumns, _)
...
val newChild = ...
i.withNewChildren(newChild :: Nil)
}
}
}
```

## 自己的解答


### Logical Plan的修改方法

可以参考[MyInsertOptimizerExtension](https://github.com/amoyyean/SparkMyOptimizerExtension/blob/master/src/main/scala/com/geektime/linyan/MyInsertOptimizerExtension.scala)和[RepartitionForInsertion](https://github.com/amoyyean/SparkMyOptimizerExtension/blob/master/src/main/scala/com/geektime/linyan/RepartitionForInsertion.scala)中的代码。


### Spark Plan的修改方法

生成1个SparkMyStrategyExtension.scala文件，内容如下

```scala
package com.geektime.linyan

import org.apache.spark.sql.SparkSessionExtensions

class SparkMyStrategyExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy { session =>
      RepartitionForInsertion(session)
    }
  }
}
```

生成1个RepartitionForInsertion.scala文件，内容如下

```scala
package com.geektime.linyan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{CoalesceExec, SparkPlan}

object RepartitionForInsertion extends Rule[SparkPlan] {
override def apply(plan: SparkPlan): SparkPlan = {
plan transformDown {
case d: DataWritingCommand =>
DataWritingCommandExec(d, planLater(plan))
case i: InsertIntoDatasource => i.withNewChildren(CoalesceExec(1, planLater(plan)))
}
}
}
```

---

## 题目一: 分析一条 TPCDS SQL (请基于 Spark 3.1.1 版本解答)

SQL从中任意选择一条：
https://github.com/apache/spark/tree/master/sql/core/src/test/resources/tpcds

1. 运行该 SQL ，如 q38 ，并截图该 SQL 的 SQL 执行图
2. 该 SQL 用到了哪些优化规则（ optimizer rules )
3. 请各用不少于 200 字描述其中的两条优化规则

0829 02:06:10

*帮助文档: 如何运行该SQL*

*1. 从 github 下载 TPCDS 数据生成器*

```shell
> git clone https://github.com/maropu/spark-tpcds-datagen.git
> cd spark-tpcds-datagen
```

*2. 下载 Spark3.1.1 到 spark tpcds datagen 目录并解压*

```shell
> wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
> tar -zxvf spark-3.1.1-bin-hadoop2.7.tgz
```

*3. 生成数据*

```shell
> mkdir -p tpcds-data-1g
> export SPARK_HOME=./spark-3.1.1-bin-hadoop2.7
> ./bin/dsdgen --output-location tpcds-data-1g
```

*4. 下载三个 test jar 并放到当前目录*

```shell
> wget https://repo1.maven.org/maven2/org/apache/spark/spark-catalyst_2.12/3.1.1/spark-catalyst_2.12-3.1.1-tests.jar
> wget https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.12/3.1.1/spark-core_2.12-3.1.1-tests.jar
> wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.1.1/spark-sql_2.12-3.1.1-tests.jar
```

*5. 执行 SQL*

```shell
> ./spark-3.1.1-bin-hadoop2.7/bin/spark-submit --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark --jars spark-core_2.12-3.1.1-tests.jar,spark-catalyst_2.12-3.1.1-tests.jar spark sql_2.12-3.1.1-tests.jar --data-location tpcds-data-1g --query-filter "q73"
```
## 题目二: 架构设计题

你是某互联网公司的大数据平台架构师，请设计一套基于 Lambda 架构的数据平台架构，要求尽可能多的把课程中涉及的组件添加到该架构图中。并描述 Lambda 架构的优缺点，要求不少于 300 字。

## 题目三 简答题（三选一）

### A. 简述 HDFS 的读写流程 要求不少于 300 字

### B. 简述 Spark Shuffle 的工作原理， 要求不少于 300 字

### C. 简述 Flink SQL 的工作原理 要求不少于 300 字

- 写出去再读回来合并
  * 优点：控制写时的文件个数
  * 缺点：小文件还是产生了，读1次小文件再写1次
  * 助教讲授时说这个方法类似第2题 Compact Table 的情况，会用到 repartition

- 写出去时合并1
  * 根据某个 key 做 distribute by , 等于加了 shuffle
  * 缺点：如果上面的操作也有shuffle, key 不一致时可能造成两次 exchange

- 写出去时合并2
  * 在物理计划里判断上次(助教讲课时改成上游或上面)的物理计划
  * 非 partition 表，可以加 round robin partition
  * 是 partition 表但没有 partition 条件，可以加 hash partition
  * 助教讲课时提到还有1种情况是 partition 表，partition 条件是上游 partition 条件(1027课程里是上游，即child计划HashPartitioning的exs的子集)，可以不用加 partition
  * 我的理解有上游的原因是因为下面的sql命令select * from t2可以是一个复杂的sql select查询，里面可能有多层包含不同 partition 的物理执行计划
  ```sql
  insert into t1 select * from t2;
  ```  
  * 助教讲授时说这个方法是老师在 ebay 的优化方案

参考文章：
1. [Apache Hudi如何智能处理小文件问题](https://www.cnblogs.com/leesf456/p/14642991.html)。助教说由于太复杂未尝试Hudi的解决方法

---

## 老师在 eBay INSERT INTO 自动合并小文件及写入数据时自动合并小文件的方法

代码可以参考 2021/10/27 课程 Delta Lake详解（下）01:21:32到01:33:55的视频和课件 17DeltaLake[1024带笔记]-极客时间训练营 的第80到83页的内容
另外简要的实现方案可以参考 2021/10/27 课程 Delta Lake详解（下）01:53:25到02:00:00的视频

运行和结果如下

![17DeltaLake_80页_Resolving_Small_Files_Problem](17DeltaLake_80页_Resolving_Small_Files_Problem.png)

![17DeltaLake_81页_Auto_Resolving_Small_Files_Problem](17DeltaLake_81页_Auto_Resolving_Small_Files_Problem.png)

![17DeltaLake_82页_Auto_Resolving_Small_Files_Problem](17DeltaLake_82页_Auto_Resolving_Small_Files_Problem.png)

![17DeltaLake_81页_Auto_Resolving_Small_Files_Problem](17DeltaLake_83页_Auto_Resolving_Small_Files_Problem.png)

