package com.ast.starters.spark

/**
  * Created by cloudera on 5/28/17.
  */
package com.test.kafka

import java.util.Properties
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructType}
import scala.util.Random

/**
  * Created by Shinelin on 5/28/17.
  */
object SparkStreamExample {

  def main(args: Array[String]): Unit = {

    val props = new Properties
    val InputStream = SparkStreamExample.getClass
      .getClassLoader
      .getResourceAsStream("../properties/prop.properties")


    props.load(InputStream)
    if (props.getProperty("Logging").toUpperCase() != "Y") {
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
    }

    val contRequestDimUser = props.getProperty("contRequestDimUser").toLowerCase
    val contRequestDimApplication = props.getProperty("contRequestDimApplication").toLowerCase
    System.setProperty("hadoop.home.dir", props.getProperty("hadoop.home.dir"))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> props.getProperty("bootstrap.servers"),
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> props.getProperty("group.id"),
      "max.partition.fetch.bytes" -> "1048576",
      "auto.offset.reset" -> "latest",
      "request.timeout.ms" -> "3050000",
      "session.timeout.ms" -> "100000",
      "heartbeat.interval.ms" -> "30000"
    )

    val conf = new SparkConf().setAppName("KafkaReceiver").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val topics = List(props.getProperty("kafkatopic")).toSet

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (props.getProperty("PerfTest").toUpperCase() == "Y") {
        rdd.foreachPartition { partition =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          //println(s"${o.topic} ${0.partition} ${0.fromOffset} ${0.untilOffset}")
        }
      }

      val foo = rdd.mapPartitions(partition => partition.map(record => record.value()))
      val spark = SparkSessionSingleton.getInstance(foo.sparkContext.getConf)
      val randomGenerator = new Random()
      import spark.implicits._

      if (props.getProperty("PerfTest").toUpperCase == "N") {
        val cntreqdimuser = foo.filter(line => line.contains(contRequestDimUser)).count()
        if (cntreqdimuser > 0) {
          val schema = ProcessDimUser.ProcessDimuserData(contRequestDimUser)
          val df = spark.read.schema(schema).json(foo).select($"requestdimuser.*")
          if (df.count() > 0) {
            println(contRequestDimUser + "Record Count: " + df.count())
            df.write.format(props.getProperty("targetfileformat")).option("delimiter",
              props.getProperty("targetfileDelimiter")).save(props.getProperty("targetdatadir") +
              contRequestDimUser + "//data_" + randomGenerator.nextInt())
          }
        }


        val cntreqdimapplication = foo.filter(line =>
          line.contains(contRequestDimApplication)).count()

        if (cntreqdimapplication > 0) {
          val schema = ProcessDimApplication.ProcessDimApplicationData(contRequestDimApplication)
          val df = spark.read.schema(schema).json(foo).select($"requestdimapplication.*")
          if (df.count() > 0) {
            println(contRequestDimApplication + "Record Count: " + df.count())
            df.write.format(props.getProperty("targetfileformat")).option("delimiter",
              props.getProperty("targetfileDelimiter")).save(props.getProperty("targetdatadir") +
              contRequestDimApplication + "//data_" + randomGenerator.nextInt())
          }
        }
      }
    }

    ssc.start();
    ssc.awaitTermination();
  }
}

object SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}

object ProcessDimUser {

  def ProcessDimuserData(processname: String): StructType = {
    val schema = (new StructType)
      .add(processname.toLowerCase, (new StructType)
        .add("DimUserID".toLowerCase, LongType)
        .add("BRID".toLowerCase, StringType)
        .add("History_Start_Date".toLowerCase, StringType)
        .add("History End Date".toLowerCase, StringType)
        .add("First_Name".toLowerCase, StringType)
        .add("Last_Name".toLowerCase, StringType)
        .add("Middle_Name".toLowerCase, StringType)
        .add("User_Type".toLowerCase, StringType)
        .add("Location".toLowerCase, StringType)
        .add("Personnel_Number".toLowerCase, StringType)
        .add("Is_Disabled".toLowerCase, BooleanType)
        .add("Status".toLowerCase, StringType)
        .add("User_Profile_CreatedOn".toLowerCase, StringType)
        .add("Employement_Type".toLowerCase, StringType)
        .add("Login".toLowerCase, StringType)
        .add("Manager_Key".toLowerCase, StringType)
        .add("Start_Date".toLowerCase, StringType)
        .add("End_Date".toLowerCase, StringType)
        .add("Email".toLowerCase, StringType)
        .add("Created_Date".toLowerCase, StringType)
        .add("CreatedBy".toLowerCase, LongType)
        .add("Updated_Date".toLowerCase, StringType)
        .add("UpdatedBy".toLowerCase, LongType)
        .add("Region".toLowerCase, StringType)
        .add("Country".toLowerCase, StringType)
        .add("City".toLowerCase, StringType)
        .add("CostCentre".toLowerCase, StringType)
        .add("Building".toLowerCase, StringType)
        .add("KnownAs".toLowerCase, StringType)
        .add("Location_Code".toLowerCase, StringType)
        .add("Telephone".toLowerCase, StringType)
        .add("Org_Unit".toLowerCase, StringType)
        .add("Corp Title".toLowerCase, StringType)
        .add("Business_Level1_Code".toLowerCase, StringType)
        .add("Business_Areal".toLowerCase, StringType)
        .add("Business_Area2".toLowerCase, StringType)
        .add("Business_Area3".toLowerCase, StringType)
        .add("Business_Area4".toLowerCase, StringType)
        .add("Move_Date".toLowerCase, StringType)
        .add("Country_Code".toLowerCase, StringType)
        .add("Disable_Date".toLowerCase, StringType)
        .add("LastDay_InOffice".toLowerCase, StringType)
        .add("Disable_PrimaryAccess_Date".toLowerCase, StringType)
        .add("Disable_SecondaryAccess_Date".toLowerCase, StringType)
        .add("Disable_Request_Number".toLowerCase, StringType)
        .add("Desk_Number".toLowerCase, StringType)
        .add("VIP".toLowerCase, StringType)
        .add("Role_Name".toLowerCase, StringType)
        .add("Org_Unit_ID".toLowerCase, StringType)
        .add("Cost_Centre_Text".toLowerCase, StringType)
        .add("Building_ID".toLowerCase, StringType)
        .add("Leaver_Notification_Date_From_HR".toLowerCase, StringType)
        .add("Leaver_Notification_Recorded_Date".toLowerCase, StringType)
        .add("DWCreate_Date".toLowerCase, StringType)
        .add("DWModify_Date".toLowerCase, StringType)
      )
    return schema
  }
}


object ProcessDimApplication {
  def ProcessDimApplicationData(processname: String): StructType = {
    val schema = (new StructType)
      .add(processname.toLowerCase, (new StructType)
        .add("DimApplicationID".toLowerCase, LongType)
        .add("History_Start_Date".toLowerCase, StringType)
        .add("History_End_Date".toLowerCase, StringType)
        .add("Application_Object_Name".toLowerCase, StringType)
        .add("Application_Type".toLowerCase, StringType)
        .add("Application_Display_Name".toLowerCase, StringType)
        .add("DWCreate_Date".toLowerCase, StringType)
        .add("DWModify_Date".toLowerCase, StringType)
      )
    return schema
  }
}

