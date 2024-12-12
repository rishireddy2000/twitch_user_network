import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.sql.types._

object TwitchNetworkProcessor {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: TwitchNetworkProcessor <kafka-brokers> <topic-name>")
      System.exit(1)
    }

    val kafkaBrokers = args(0)
    val topicName = args(1)

    println(s"[${java.time.LocalDateTime.now}] Starting Twitch Network Processor")
    println(s"[${java.time.LocalDateTime.now}] Using Kafka brokers: $kafkaBrokers")
    println(s"[${java.time.LocalDateTime.now}] Using topic: $topicName")

    val spark = SparkSession.builder()
      .appName("TwitchNetworkProcessor")
      .enableHiveSupport()
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.default.parallelism", 10)
      .getOrCreate()

    import spark.implicits._

    // Kafka configuration
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "twitch-network-processor",
      "auto.offset.reset" -> "latest"
    )

    // Define the schema for network edges
    val edgeSchema = StructType(Seq(
      StructField("user_id_1", LongType, nullable = false),
      StructField("user_id_2", LongType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    ))

    def loadHiveTable(tableName: String): DataFrame = {
      println(s"[${java.time.LocalDateTime.now}] Loading Hive table: $tableName")
      val df = spark.read
        .format("jdbc")
        .option("url", "jdbc:hive2://10.0.0.50:10001/;transportMode=http")
        .option("dbtable", tableName)
        .option("fetchsize", "10000")
        .option("batchsize", "10000")
        .load()
      println(s"[${java.time.LocalDateTime.now}] Successfully loaded $tableName")
      df
    }

    val initialEdges = loadHiveTable("twitch_edges_hbase").cache()
    val initialFeatures = loadHiveTable("twitch_features_hbase").cache()

    println(s"[${java.time.LocalDateTime.now}] Loaded ${initialEdges.count()} edges")
    println(s"[${java.time.LocalDateTime.now}] Loaded ${initialFeatures.count()} features")

    // Debug output for schema
    println("Schema for initialEdges:")
    initialEdges.printSchema()

    val streamingDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .load()

    val query = streamingDF
      .selectExpr("CAST(value AS STRING)")
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          println(s"[${java.time.LocalDateTime.now}] Processing batch $batchId")
          println(s"[${java.time.LocalDateTime.now}] Received ${batchDF.count()} records")

          println(s"[${java.time.LocalDateTime.now}] Converting batch to edges format...")
          val newEdges = batchDF.select(
            from_json(col("value"), edgeSchema).as("data")
          ).select("data.*")

          println(s"[${java.time.LocalDateTime.now}] Creating first level connections RDD...")
          val firstLevelUpdates = newEdges.rdd
            .map(row => {
              val userId1 = row.getAs[Long]("user_id_1")
              val userId2 = row.getAs[Long]("user_id_2")
              ((userId1, userId2), 1L)
            })
            .reduceByKey(_ + _)
            .map { case ((source, friend), count) =>
              (source, friend, count)
            }
            .toDF("source_user", "friend", "connection_strength")

          println(s"[${java.time.LocalDateTime.now}] Writing first level connections to Hive...")

          if (!spark.catalog.tableExists("twitch_first_level_connections")) {
            firstLevelUpdates.write
              .mode(SaveMode.Overwrite)
              .saveAsTable("twitch_first_level_connections")
          } else {
            firstLevelUpdates.write
              .mode(SaveMode.Append)
              .insertInto("twitch_first_level_connections")
          }

          // Print batch summary
          println(s"[${java.time.LocalDateTime.now}] Batch $batchId processing complete")
          println(s"[${java.time.LocalDateTime.now}] Summary:")
          println(s"  - Received records: ${batchDF.count()}")
          println(s"  - First level connections processed: ${firstLevelUpdates.count()}")
          println("----------------------------------------")
        } else {
          println(s"[${java.time.LocalDateTime.now}] Received empty batch $batchId")
        }
      }
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    println(s"[${java.time.LocalDateTime.now}] Streaming query started. Waiting for data...")
    query.awaitTermination()
  }
}