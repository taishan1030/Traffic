package com.wls.learn.monitor

import java.util.Properties

import com.wls.learn.utils.{AvgSpeedInfo, TrafficInfo, WriteDataSink}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

/**
 * @Author:wangpeng
 * @Date: 2022/12/5
 * @Description: 卡口车辆平均速度监控
 * @version:1.0
 */
object AvgSpeedByMonitorAnalysis {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("group.id","msb_001")

    //创建一个Kafka的Source
//    val stream1:DataStream[TrafficInfo] = streamEnv.addSource(
//      new FlinkKafkaConsumer[String]("topic_monitor_info", new SimpleStringSchema(),props).setStartFromEarliest()
//    ).map(line=> {
//      var arr = line.split(",")
//      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
//    })//引入Watermark，并且延迟时间为5秒
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TrafficInfo](Time.seconds(5)) {
//        override def extractTimestamp(element: TrafficInfo) = element.actionTime
//     })

     //方便测试 nc -lp 9999   linux: nc -lp 9999
    var stream1:DataStream[TrafficInfo] = streamEnv.socketTextStream("localhost",9999).map(line=> {
      var arr = line.split(",")
      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
    })//引入Watermark，并且延迟时间为5秒
     .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TrafficInfo](Time.seconds(5)) {
        override def extractTimestamp(element: TrafficInfo) = element.actionTime
     })

    stream1.keyBy(_.monitorId)
      .timeWindow(Time.minutes(5),Time.minutes(1)) //定义 滑动窗口, 窗口大小5分钟, 每1分钟滑动一次
      .aggregate(//设计一个累加器：二元组(车速之后，车辆的数量)
         new AggregateFunction[TrafficInfo,(Double,Long),(Double,Long)] {
           override def createAccumulator(): (Double, Long) = (0,0)

           override def add(value: TrafficInfo, acc: (Double, Long)): (Double, Long) = {(acc._1+value.speed,acc._2+1)}

           override def getResult(acc: (Double, Long)): (Double, Long) = acc

           override def merge(a: (Double, Long), b: (Double, Long)): (Double, Long) = {(a._1+b._1,a._2+b._2)}
         },
        (k:String, w:TimeWindow,input: Iterable[(Double,Long)],out: Collector[AvgSpeedInfo]) => {
          val acc: (Double, Long) = input.last
          var avg :Double  =(acc._1/acc._2).formatted("%.2f").toDouble
          out.collect(new AvgSpeedInfo(w.getStart,w.getEnd,k,avg,acc._2.toInt))
        }
      )
//      .print()
      .addSink(new WriteDataSink[AvgSpeedInfo](classOf[AvgSpeedInfo]))

      streamEnv.execute()
  }
}
