# Flink on Kinesis Data Analytics Demo
本项目是一个java-scala混合结构，由于demo逻辑比较简单，只用了scala部分

整个流应用参考了 [cake patten](https://www.baeldung.com/scala/cake-pattern#:~:text=It%E2%80%99s%20called%20the%20Cake%20pattern%2C%20a%20sort%20of,The%20first%20is%20how%20to%20declare%20a%20dependency.)

在正式项目中，可以把业务流程写在Scala中，一些非业务逻辑写在java部分
##  项目结构

```dtd
scala
--cn.nwcd.presales
----common              # 存放各个流应用公用的一些代码模块
----patpat              # 某个业务场景包，包名以业务名为准，如果有新的业务，可以在cn.nwcd.presales下创建新包
------app               # 存放各个流应用的入口文件，如果要切换，需要更新pom.xml文件的 mainClass
------entity            # 同一个业务场景下的公用数据实体
------metric            # demo 流式应用，目前是一个数据指标看板，所以起名为metric,故对应的入口文件是app/MetricApp
-----------Input        # 读取输入流的逻辑
-----------Compute      # 业务计算逻辑
-----------Output       # 编写输出的逻辑
-----------Params       # 应用中存放参数的地方，或在这里编写获取参数的逻辑，例如 输入流名称
-----------Context      # 设置当前流应用上下文逻辑
------util              # 当前业务场景下的工具类
```
demo 应用 MetricApp的逻辑流程
```shell
Input -> Compute -> Output
```
各个部分的的流消息传递通过函数 setDataSet 和 getDataSet 实现
```shell
setDataSet("stock_input_events", watermarkEvent)

val rawStockEventDs: DataStream[StockRawEvent] = getDataSet[DataStream[StockRawEvent]]("stock_input_events")
    
```


