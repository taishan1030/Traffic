package com.wls.learn.utils

import java.util

import org.apache.flink.configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager, HTableInterface, Put}

/**
 * @Author:wangpeng
 * @Date: 2022/12/11
 * @Description: ***
 * @version:1.0
 */
class HbaseWriteDataSink extends RichSinkFunction[java.util.List[Put]]{

  var conn :HConnection=_
  var conf :Configuration=_

  //初始化hbase的连接
  override def open(parameters: configuration.Configuration): Unit = {
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","hadoop106:2181")
    conn = HConnectionManager.createConnection(conf) //hbase数据库连接池
  }

  override def close(): Unit = {
    conn.close()
  }

  //往Hbase中写数据
  override def invoke(value: util.List[Put], context: SinkFunction.Context[_]): Unit = {
    val hTableInterface:HTableInterface = conn.getTable("t_track_info")
    hTableInterface.put(value)
    hTableInterface.close()
  }
}

