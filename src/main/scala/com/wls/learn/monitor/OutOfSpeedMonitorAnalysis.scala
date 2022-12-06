package com.wls.learn.monitor

import java.util.Properties

import com.wls.learn.utils.{GlobalConstants, JdbcReadDataSource, MonitorInfo, OutOfLimitSpeedInfo, TrafficInfo, WriteDataSink}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * @Author:wangpeng
 * @Date: 2022/12/4
 * @Description: 实时车辆超速监控
 * @version:1.0
 */
object OutOfSpeedMonitorAnalysis {
  def main(args: Array[String]): Unit={
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("group.id","msb_001")

    //创建一个Kafka的Source
    //stream1海量的数据流，不可以存入广播状态流中
//    val stream1:DataStream[TrafficInfo] = streamEnv.addSource(
//      new FlinkKafkaConsumer[String]("topic_monitor_info", new SimpleStringSchema(),props).setStartFromEarliest()
//    ).map(line=> {
//      var arr = line.split(",")
//      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
//    })

    // 方便测试 nc -lp 9999   linux: nc -lp 9999
    var stream1:DataStream[TrafficInfo] = streamEnv.socketTextStream("localhost",9999).map(line=> {
      var arr = line.split(",")
      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
    })

    //stream2 从Mysql数据库中读取的卡口限速信息，特点：数据量少，更新不频繁
    var stream2:BroadcastStream[MonitorInfo] = streamEnv.addSource(
      new JdbcReadDataSource[MonitorInfo](classOf[MonitorInfo])
    ).broadcast(GlobalConstants.MONITOR_STATE_DESCRIPTOR)

    //Flink中有connect ：内和外都可以  和join ：内连接
    stream1.connect(stream2)
      .process(new BroadcastProcessFunction[TrafficInfo,MonitorInfo,OutOfLimitSpeedInfo] {
        override def processElement(value: TrafficInfo, ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#ReadOnlyContext, out: Collector[OutOfLimitSpeedInfo]): Unit = {
          //先从状态中得到当前卡口的限速信息
          var info:MonitorInfo = ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR).get(value.monitorId)
          if (info != null) { //表示当前这里车，经过的卡口是有限速的
            var limitSpeed = info.limitSpeed
            var realSpeed = value.speed
            if(limitSpeed < 1.1 * realSpeed) {//当前车辆超速通过卡口
              out.collect(new OutOfLimitSpeedInfo(value.car,value.monitorId,value.roadId,realSpeed,limitSpeed,value.actionTime))
            }
          }
        }

        override def processBroadcastElement(value: MonitorInfo, ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#Context, collector: Collector[OutOfLimitSpeedInfo]): Unit = {
          //把广播流中的数据保存到状态中
          ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR).put(value.monitorId,value)
        }
      }) //.print()
        .addSink(new WriteDataSink[OutOfLimitSpeedInfo](classOf[OutOfLimitSpeedInfo])) // sink


    streamEnv.execute()
  }
}
