package com.gec.process

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gec.util.OffsetUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object Covid19_WZData_Process {

  def main(args: Array[String]): Unit = {

    //1.准备SparkStreaming的开发环境
    val conf: SparkConf = new SparkConf().setAppName("Covid19_WZData_Process").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./sscckp")

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop121:9092", //kafka集群地址
      "group.id" -> "SparkKafka",
      //latest表示如果记录了偏移量则从记录的位置开始消费,如果没有记录则从最新/最后的位置开始消费
      //earliest表示如果记录了偏移量则从记录的位置开始消费,如果没有记录则从最开始/最早的位置开始消费
      //none表示如果记录了偏移量则从记录的位置开始消费,如果没有记录则报错
      "auto.offset.reset" -> "latest", //偏移量重置位置
      "enable.auto.commit" -> (false: java.lang.Boolean), //是否自动提交偏移量
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )

    val topics: Array[String] = Array("covid19_wz")

    //从MySQL中查询出offsets:Map[TopicPartition, Long]
    val offsetsMap: mutable.Map[TopicPartition, Long] = OffsetUtils.getOffsetsMap("SparkKafka", "covid19_wz")

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = if (offsetsMap.size > 0) {
      println("MySQL记录了offset信息,从offset处开始消费!")
      //3.连接kafka获取消息
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetsMap))
    } else {
      println("MySQL没有记录offset信息,从latest处开始消费")
      //3.连接kafka获取消息
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    }


    val tupleDS: DStream[(String, (Int, Int, Int, Int, Int, Int))] = kafkaDS.map(record => {
      val jsonStr: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      val name: String = jsonObj.getString("name")
      val from: String = jsonObj.getString("from")
      val count: Int = jsonObj.getInteger("count")
      //根据物资来源不同,将count记在不同的位置,最终形成统一的格式
      from match {
        case "采购" => (name, (count, 0, 0, 0, 0, count))
        case "下拨" => (name, (0, count, 0, 0, 0, count))
        case "捐赠" => (name, (0, 0, count, 0, 0, count))
        case "消耗" => (name, (0, 0, 0, -count, 0, -count))
        case "需求" => (name, (0, 0, 0, 0, -count, -count))
      }
    })

    //4.2将上述格式的数据按照key进行聚合(有状态的计算)--使用updateStateBykey
    //定义一个函数,用来将当前批次的数据和历史数据进行聚合
    val updateFunc = (currentValues: Seq[(Int, Int, Int, Int, Int, Int)], historyValue: Option[(Int, Int, Int, Int, Int, Int)]) => {
      //0.定义变量用来接收当前批次数据(采购,下拨,捐赠,消耗,需求,库存)
      var current_cg: Int = 0
      var current_xb: Int = 0
      var current_jz: Int = 0
      var current_xh: Int = 0
      var current_xq: Int = 0
      var current_kc: Int = 0
      if (currentValues.size > 0) {
        //1.取出当前批次数据
        for (currentValue <- currentValues) {
          current_cg += currentValue._1
          current_xb += currentValue._2
          current_jz += currentValue._3
          current_xh += currentValue._4
          current_xq += currentValue._5
          current_kc += currentValue._6
        }
        //2.取出历史数据
        val history_cg: Int = historyValue.getOrElse((0, 0, 0, 0, 0, 0))._1
        val history_xb: Int = historyValue.getOrElse((0, 0, 0, 0, 0, 0))._2
        val history_jz: Int = historyValue.getOrElse((0, 0, 0, 0, 0, 0))._3
        val history_xh: Int = historyValue.getOrElse((0, 0, 0, 0, 0, 0))._4
        val history_xq: Int = historyValue.getOrElse((0, 0, 0, 0, 0, 0))._5
        val history_kc: Int = historyValue.getOrElse((0, 0, 0, 0, 0, 0))._6

        //3.将当前批次数据和历史数据进行聚合
        val result_cg: Int = current_cg + history_cg
        val result_xb: Int = current_xb + history_xb
        val result_jz: Int = current_jz + history_jz
        val result_xh: Int = current_xh + history_xh
        val result_xq: Int = current_xq + history_xq
        val result_kc: Int = current_kc + history_kc

        //4.将聚合结果进行返回
        Some((
          result_cg,
          result_xb,
          result_jz,
          result_xh,
          result_xq,
          result_kc))
      } else {
        historyValue
      }
    }
    val resultDS: DStream[(String, (Int, Int, Int, Int, Int, Int))] = tupleDS.updateStateByKey(updateFunc)
    //resultDS.print()

    // 将处理分析的结果存入到MySQL中
    resultDS.foreachRDD(rdd => {
      rdd.foreachPartition(lines => {
        //1.开启连接
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/covid19?characterEncoding=UTF-8", "root", "root")
        //2.编写sql并获取ps
        val sql: String = "replace into covid19_wz(name,cg,xb,jz,xh,xq,kc) values(?,?,?,?,?,?,?)"
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //3.设置参数并执行
        for (line <- lines) {
          ps.setString(1,line._1)
          ps.setInt(2,line._2._1)
          ps.setInt(3,line._2._2)
          ps.setInt(4,line._2._3)
          ps.setInt(5,line._2._4)
          ps.setInt(6,line._2._5)
          ps.setInt(7,line._2._6)
          ps.executeUpdate()
        }
        //4.关闭资源
        ps.close()
        conn.close()
      })
    })

    //6.手动提交偏移量
    //我们要手动提交偏移量,那么就意味着,消费了一批数据就应该提交一次偏移量
    //在SparkStreaming中数据抽象为DStream,DStream的底层其实也就是RDD,也就是每一批次的数据
    //所以接下来我们应该对DStream中的RDD进行处理
    kafkaDS.foreachRDD(rdd => {
      if (rdd.count() > 0) { //如果该rdd中有数据则处理
        //rdd.foreach(record=>println("从Kafka中消费到的每一条消息:"+record))
        //从Kafka中消费到的每一条消息:ConsumerRecord(topic = covid19_wz, partition = 2, offset = 19, CreateTime = 1590634048100, checksum = 1933585894, serialized key size = -1, serialized value size = 3, key = null, value = 789)
        //获取偏移量
        //使用Spark-streaming-kafka-0-10中封装好的API来存放偏移量并提交
        val offsets: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //for (o <-offsets){
        //println(s"topic=${o.topic},partition=${o.partition},fromOffset=${o.fromOffset},until=${o.untilOffset}")
        //topic=covid19_wz,partition=1,fromOffset=12,until=14
        //topic=covid19_wz,partition=0,fromOffset=16,until=18
        //topic=covid19_wz,partition=2,fromOffset=19,until=21
        // }
        //手动提交偏移量到Kafka的默认主题:__consumer__offsets中,如果开启了Checkpoint还会提交到Checkpoint中
        //kafkaDS.asInstanceOf[CanCommitOffset].commitAsync(offsets)
        OffsetUtils.saveOffsets("SparkKafka", offsets)
      }
    })

    //7.开启SparkStreaming任务并等待结束
    ssc.start()
    ssc.awaitTermination()

  }

}
