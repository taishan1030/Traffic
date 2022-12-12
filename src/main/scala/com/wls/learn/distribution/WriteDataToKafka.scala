package com.wls.learn.distribution

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties

import akka.remote.serialization.StringSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @Author:wangpeng
 * @Date: 2022/12/12
 * @Description: ***
 * @version:1.0
 */
object WriteDataToKafka {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("key.serializer", classOf[StringSerializer].getName)
    props.setProperty("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String,String](props)
    var in = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\IdeaProjects\\trafficmonitor_msb_2\\src\\main\\resources\\log_2020-06-21_0.log")))
    var line =in.readLine()
    while (line!=null){
      val record:ProducerRecord[String,String] = new ProducerRecord[String,String]("t_traffic_msb",null,line)
      producer.send(record)
      line =in.readLine()
    }
    in.close()
    producer.close()
  }
}
