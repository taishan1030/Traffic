package com.wls.learn.distribution

import java.util.Properties

import com.wls.learn.utils.TrafficInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @Author:wangpeng
 * @Date: 2022/12/11
 * @Description: ***
 * @version:1.0
 */
object AreaDistributionAnalysis {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setParallelism(1)
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("group.id","msb_001")

    //创建一个Kafka的Source
//    val stream: DataStream[TrafficInfo] = streamEnv.addSource(
//      new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema(), props).setStartFromEarliest() //从第一行开始读取数据
//    )
//      .map(line => {
//        var arr = line.split(",")
//        new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
//      })

    val stream: DataStream[TrafficInfo] = streamEnv.socketTextStream("localhost",9999)
      .map(line => {
        var arr = line.split(",")
        new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      })

    stream.keyBy(_.areaId)
      .timeWindow(Time.seconds(10))
      .apply(
        (k:String, window:TimeWindow, input:Iterable[TrafficInfo],out:Collector[String]) => {
          val set:mutable.Set[String] = scala.collection.mutable.Set() //Set集合去重
          for (i<-input) {
            set += i.car
          }
          out.collect(s"区域${k},在窗口其实时间${window.getStart},到窗口结束时间${window.getEnd} ,一共有${set.size} 辆车")
        }
      )
      .print()
    streamEnv.execute()
  }

}
