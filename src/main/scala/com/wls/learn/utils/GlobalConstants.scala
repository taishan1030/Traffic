package com.wls.learn.utils

import org.apache.flink.api.common.state.MapStateDescriptor

/**
 * @Author:wangpeng
 * @Date: 2022/12/4
 * @Description: ***
 * @version:1.0
 */
//样例类
//从Kafka中读取的数据，车辆经过卡口的信息
case class TrafficInfo(actionTime:Long,monitorId:String,cameraId:String,car:String, speed:Double, roadId:String,areaId:String)

//卡口信息的样例类
case class MonitorInfo(monitorId:String, roadId:String,limitSpeed:Int,areaId:String)

//车辆超速的信息
case class OutOfLimitSpeedInfo(car:String,monitorId:String,roadId:String,realSpeed:Double,limitSpeed:Int,actionTime:Long)

//某个时间范围内卡口的平均车速和通过的车辆数量
case class AvgSpeedInfo(start:Long,end:Long,monitorId:String,avgSpeed:Double,carCount:Int)


object GlobalConstants {
  lazy val MONITOR_STATE_DESCRIPTOR =new MapStateDescriptor[String,MonitorInfo]("monitor_info",classOf[String],classOf[MonitorInfo])
}
