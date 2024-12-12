import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object TwitchNetworkProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TwitchNetworkProcessor")
      .enableHiveSupport()  // Enable Hive support
      .getOrCreate()

    // Load initial data
    def loadHiveTable(tableName: String): DataFrame = {
      val prefix = s"$tableName."
      var df = spark.read
        .format("jdbc")
        .option("url", "jdbc:hive2://headnodehost:10001/;transportMode=http")
        .option("dbtable", tableName)
        .load()

      df.columns.foreach { colName =>
        if (colName.startsWith(prefix)) {
          df = df.withColumnRenamed(colName, colName.stripPrefix(prefix))
        }
      }
      df
    }

    // Load initial data
    val edges = loadHiveTable("twitch_edges_hbase")
    val features = loadHiveTable("twitch_features_hbase")

    // Create temp views for SQL operations
    edges.createOrReplaceTempView("edges")
    features.createOrReplaceTempView("features")

    // Process first level connections
    val firstLevelConnections = spark.sql("""
      SELECT
        user_id_1 as source_user,
        user_id_2 as friend,
        COUNT(*) as connection_strength
      FROM edges
      GROUP BY user_id_1, user_id_2
    """)

    // Save first level connections to Hive
    firstLevelConnections.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("twitch_first_level_connections")

    // Process second level connections
    val secondLevelConnections = spark.sql("""
      SELECT DISTINCT
        e1.user_id_1 as source_user,
        e2.user_id_2 as friend_of_friend
      FROM edges e1
      JOIN edges e2 ON e1.user_id_2 = e2.user_id_1
      WHERE e1.user_id_1 != e2.user_id_2
    """)

    // Save second level connections to Hive
    secondLevelConnections.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("twitch_second_level_connections")

    // Process user statistics
    val userStats = spark.sql("""
      SELECT
        e.user_id_1,
        COUNT(DISTINCT e.user_id_2) as friend_count,
        f.views,
        f.language,
        f.life_time,
        f.affiliate
      FROM edges e
      JOIN features f ON CAST(e.user_id_1 AS STRING) = f.row_key
      GROUP BY
        e.user_id_1,
        f.views,
        f.language,
        f.life_time,
        f.affiliate
    """)

    // Save user stats to Hive
    userStats.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("twitch_user_stats")

    // Process and store network metrics
    spark.sql("""
      SELECT
        e.user_id_1 as user_id,
        COUNT(DISTINCT e.user_id_2) as direct_connections,
        AVG(f.views) as avg_friend_views,
        COUNT(DISTINCT sl.friend_of_friend) as indirect_connections
      FROM edges e
      LEFT JOIN features f ON CAST(e.user_id_2 AS STRING) = f.row_key
      LEFT JOIN second_level_connections sl ON e.user_id_1 = sl.source_user
      GROUP BY e.user_id_1
    """).write
      .mode(SaveMode.Overwrite)
      .saveAsTable("twitch_network_metrics")

    // Function to update tables
    def updateHiveTables(): Unit = {
      println(s"Starting update at ${java.time.LocalDateTime.now()}")

      // Reload data
      val newEdges = loadHiveTable("twitch_edges")
      val newFeatures = loadHiveTable("twitch_features")

      newEdges.createOrReplaceTempView("edges")
      newFeatures.createOrReplaceTempView("features")

      // Update first level connections
      spark.sql("""
        SELECT
          user_id_1 as source_user,
          user_id_2 as friend,
          COUNT(*) as connection_strength
        FROM edges
        GROUP BY user_id_1, user_id_2
      """).write
        .mode(SaveMode.Overwrite)
        .saveAsTable("twitch_first_level_connections")

      // Update second level connections
      spark.sql("""
        SELECT DISTINCT
          e1.user_id_1 as source_user,
          e2.user_id_2 as friend_of_friend
        FROM edges e1
        JOIN edges e2 ON e1.user_id_2 = e2.user_id_1
        WHERE e1.user_id_1 != e2.user_id_2
      """).write
        .mode(SaveMode.Overwrite)
        .saveAsTable("twitch_second_level_connections")

      // Update user stats
      spark.sql("""
        SELECT
          e.user_id_1,
          COUNT(DISTINCT e.user_id_2) as friend_count,
          f.views,
          f.language,
          f.life_time,
          f.affiliate
        FROM edges e
        JOIN features f ON CAST(e.user_id_1 AS STRING) = f.row_key
        GROUP BY
          e.user_id_1,
          f.views,
          f.language,
          f.life_time,
          f.affiliate
      """).write
        .mode(SaveMode.Overwrite)
        .saveAsTable("twitch_user_stats")

      println(s"Completed update at ${java.time.LocalDateTime.now()}")
    }

    // Keep the application running
    println("Starting continuous update loop")
    while(true) {
      updateHiveTables()
      Thread.sleep(60000) // Update every minute
    }
  }
}