package com.wls.learn.warning

import com.wls.learn.utils.{PoliceAction, ViolationInfo}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{KeyedCoProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author:wangpeng
 * @Date: 2022/12/11
 * @Description: ***
 * @version:1.0
 */
/**
 * 违法车辆和交警出警分析
 * 第一种，当前的违法车辆（在5分钟内）如果已经出警了。（最后输出道主流中做删除处理）。
 * 第二种，当前违法车辆（在5分钟后）交警没有出警（发出出警的提示，在侧流中发出）。
 * 第三种，有交警的出警记录，但是不是由监控平台报的警。
 * 需要两种数据流：
 * 1、系统的实时违法车辆的数据流
 * 2、交警实时出警记录数据
 */

object ViolationCarAndPoliceActionAnalysis2 {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val stream1: DataStream[ViolationInfo] = streamEnv.socketTextStream("hadoop101", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new ViolationInfo(arr(0), arr(1), arr(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ViolationInfo](Time.seconds(2)) {
        override def extractTimestamp(element: ViolationInfo) = element.createTime
      })

    val stream2: DataStream[PoliceAction] = streamEnv.socketTextStream("hadoop101", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new PoliceAction(arr(0), arr(1), arr(2), arr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PoliceAction](Time.seconds(2)) {
      override def extractTimestamp(element: PoliceAction) = element.actionTime
    })

    val tag1 = new OutputTag[PoliceAction]("No Violaction Car!")
    val tag2 = new OutputTag[ViolationInfo]("No PoliceAction!")

    //外连接
    var mainStream = stream1.keyBy(_.car)
      .connect(stream2.keyBy(_.car)) //
      .process(new KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String] {
        //需要两个状态，分别保存违法数据，出警记录
        lazy val violationState: ValueState[ViolationInfo] = getRuntimeContext.getState(new ValueStateDescriptor[ViolationInfo]("violate", classOf[ViolationInfo]))
        lazy val policaState: ValueState[PoliceAction] = getRuntimeContext.getState(new ValueStateDescriptor[PoliceAction]("police", classOf[PoliceAction]))

        override def processElement1(value: ViolationInfo, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#Context, out: Collector[String]): Unit = {
          //得到一个违法车辆，需要从状态中判断是否有对应的出警记录，1 状态中没有出警的记录，注册一个触发器，5分钟之后触发
          val policeAction: PoliceAction = policaState.value()
          if (policeAction == null) { //可能出警的数据还没有读到，或者该违法处理还没有交警出警
            ctx.timerService().registerEventTimeTimer(value.createTime + 5000) //5秒后触发提示
            violationState.update(value)
          } else { //已经有一条与之对应的出警记录,可以关联
            out.collect(s"该违法车辆${value.car}，违法时间${value.createTime},已经有交警出警了，警号为:${policeAction.policeId},出警的状态是：${policeAction.actionStatus},出警的时间:${policeAction.actionTime}")
            violationState.clear()
            policaState.clear()
          }
        }

        //当从第二个流中读取一条出警记录数据
        override def processElement2(value: PoliceAction, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#Context, out: Collector[String]): Unit = {
          // 得到一个出警的记录， 如果在状态中有与之对应的违法车辆数据，删除触发器
          val violationInfo: ViolationInfo = violationState.value()
          if (violationInfo == null) { //出警记录没有找到对应的违法车辆信息
            ctx.timerService().registerEventTimeTimer(value.actionTime + 5000)
            policaState.update(value)
          } else { //已经有一条与之对应的出警记录,可以关联
            out.collect(s"该违法车辆${violationInfo.car}，违法时间${violationInfo.createTime},已经有交警出警了，警号为:${value.policeId},出警的状态是：${value.actionStatus},出警的时间:${value.actionTime}")
            violationState.clear()
            policaState.clear()
          }
        }

        //触发器触发的函数
        override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#OnTimerContext, out: Collector[String]) = {
          val violationInfo: ViolationInfo = violationState.value()
          val policeAction: PoliceAction = policaState.value()
          if (violationInfo == null && policeAction != null) { //表示有出警记录，但是没有匹配的违法车辆
            ctx.output(tag1, policeAction)
          }
          if (policeAction == null && violationInfo != null) { //有违法车辆信息，但是5分钟内还没有出警记录
            ctx.output(tag2, violationInfo)
          }
          //清空状态
          violationState.clear()
          policaState.clear()
        }
      })
    mainStream.print()
    mainStream.getSideOutput(tag1).print("没有对应的违法车辆信息")
    mainStream.getSideOutput(tag2).print("该违法车辆在5分钟内没有交警出警")
    streamEnv.execute()
  }
}
