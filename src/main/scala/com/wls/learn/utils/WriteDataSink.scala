package com.wls.learn.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * @Author:wangpeng
 * @Date: 2022/12/4
 * @Description: ***
 * @version:1.0
 */
class WriteDataSink[T](classType:Class[_<:T]) extends RichSinkFunction[T]{

  var conn :Connection =_
  var pst :PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/traffic_monitor","root","123456")
    if(classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)){
      pst = conn.prepareStatement("insert into t_speeding_info (car,monitor_id,road_id,real_speed,limit_speed,action_time) values (?,?,?,?,?,?)")
    }
    if(classType.getName.equals(classOf[AvgSpeedInfo].getName)) {
      pst = conn.prepareStatement("insert into t_average_speed (start_time,end_time,monitor_id,avg_speed,car_count) values (?,?,?,?,?)")
    }
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }

  override def invoke(value: T, ctx: SinkFunction.Context[_]): Unit ={
    if (classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)) {
      var info: OutOfLimitSpeedInfo = value.asInstanceOf[OutOfLimitSpeedInfo]
      pst.setString(1,info.car)
      pst.setString(2,info.monitorId)
      pst.setString(3,info.roadId)
      pst.setDouble(4,info.realSpeed)
      pst.setInt(5,info.limitSpeed)
      pst.setLong(6,info.actionTime)
      pst.executeUpdate()
    }
    if (classType.getName.equals(classOf[AvgSpeedInfo].getName)) {
      val info: AvgSpeedInfo = value.asInstanceOf[AvgSpeedInfo]
      pst.setLong(1,info.start)
      pst.setLong(2,info.end)
      pst.setString(3,info.monitorId)
      pst.setDouble(4,info.avgSpeed)
      pst.setInt(5,info.carCount)
      pst.executeUpdate()
    }
  }
}
