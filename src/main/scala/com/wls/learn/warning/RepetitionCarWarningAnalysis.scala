package com.wls.learn.warning

import java.util.Properties

import com.wls.learn.utils.{RepetitionCarWarning, TrafficInfo, WriteDataSink}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * @Author:wangpeng
 * @Date: 2022/12/8
 * @Description: 套牌分析, 10s内通过两个卡口
 * @version:1.0
 */
object RepetitionCarWarningAnalysis {

  def main(args: Array[String]): Unit = {
    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("group.id","msb_001")

//    var stream:DataStream[TrafficInfo] = streamEnv.addSource(
//      new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema(),props).setStartFromEarliest()
//    ).map(line=>{
//      var arr = line.split(",")
//      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
//    })
//      //引入时间时间
//      .assignAscendingTimestamps(_.actionTime)

    //方便测试 nc -lp 9999   linux: nc -lp 9999
    var stream1:DataStream[TrafficInfo] = streamEnv.socketTextStream("localhost",9999).map(line=> {
      var arr = line.split(",")
      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
    })//引入时间时间
     .assignAscendingTimestamps(_.actionTime)

    stream1.keyBy(_.car)//根据车牌号分
      .process(new KeyedProcessFunction[String, TrafficInfo, RepetitionCarWarning] {
        //状态中保存第一辆出现的信息对象
        lazy val firstState = getRuntimeContext.getState(new ValueStateDescriptor[TrafficInfo]("first",classOf[TrafficInfo]))
        override def processElement(i: TrafficInfo, context: KeyedProcessFunction[String, TrafficInfo, RepetitionCarWarning]#Context, collector: Collector[RepetitionCarWarning]): Unit = {
          val first :TrafficInfo =firstState.value()
          if (first == null) {  //当前这量车就是第一辆车
            firstState.update(i)
          }else {//有两辆车出现了，但是时间已经超过了10秒，另外一种情况是时间没有超过10秒(涉嫌套牌)
            val nowTime = i.actionTime
            val firstTime = first.actionTime
            var less:Long = (nowTime - firstTime).abs /1000
            if (less <= 10 ) {//涉嫌
              var warn = new RepetitionCarWarning(i.car,if(nowTime>firstTime) first.monitorId else i.monitorId,
                if(nowTime<firstTime) first.monitorId else i.monitorId, "涉嫌套牌车",
                context.timerService().currentProcessingTime()
              )
              collector.collect(warn)
              firstState.clear()
            }else { //不是套牌车，把第二次经过卡口的数据保存到状态中，以便下次判断
              if(nowTime>firstTime) firstState.update(i)
            }
          }
        }
      })
      .addSink(new WriteDataSink[RepetitionCarWarning](classOf[RepetitionCarWarning]))

    streamEnv.execute()
  }


}
