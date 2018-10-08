package com.techlab.spark

import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.sql.{SaveMode, SparkSession}

object tweetEngine {

  def main(args: Array[String]) {


    val spark = SparkSession.builder
      .master("local")
      .appName("stream-2-spark")
      .getOrCreate()

    val sc = spark.sparkContext

    val ssc  = new StreamingContext(sc,Seconds(5))

    //read from kafka topics set topics list
    val topics = List("NIFIINPUT","PYINPUT").toSet

    val kafkaParams = Map(
      "bootstrap.servers"-> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "mygroup_id",
      "auto.offset.reset" -> "earliest")

    val line = KafkaUtils
      .createDirectStream[String,String](ssc, PreferConsistent, ConsumerStrategies
        .Subscribe[String, String](topics, kafkaParams))

    val lineMapper = line.map(record=>record.value().toString.toLowerCase)
    val lineFlatMap = lineMapper.flatMap(_.split(" "))

    val hashtags = lineFlatMap.filter(lineFlatMap => lineFlatMap.startsWith("#"))
    val topCounts = hashtags
      .reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    //lineMapper.print

    lineFlatMap.print

    topCounts.print


    ssc.start()
    ssc.awaitTermination

   /* //Initialize Spark by creating Spark config and Spark context object
    val conf = new SparkConf()
    //set Master and app name
    conf.setMaster("local")
    conf.setAppName("stream-2-spark")

    //create spark context object
    val sc = new SparkContext(conf)

    //create RDD
    val rdd = sc.makeRDD(Array(1,2,3,4,5,6))
    //display RDD
    rdd.collect().foreach(println)*/
  }
}
