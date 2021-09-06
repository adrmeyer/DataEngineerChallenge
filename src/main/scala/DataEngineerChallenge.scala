
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DataEngineerChallenge {

  // I decided to choose 1800 seconds (30 minutes) as the inactivity threshold for a session as it is commonly described
  // as the industry standard.
  val inactivityThreshold = 1800

  def parseLogLine(line: String): LogRecord = {
    val pattern = """^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r
    val res = pattern.findFirstMatchIn(line)

    res match {
      case Some(m) =>
        LogRecord(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9),
          m.group(10), m.group(11), m.group(12), m.group(13), m.group(14), m.group(15))
      case None =>
        println("The following log record couldn't be parsed: " + line)
        LogRecord("", "-", "-", "", "", "", "", "-", "-", "", "", "", "", "", "")
    }
  }

  // This function checks if the duration since the last request is over the inactivity threshold. If this period of
  // inactivity is reached, further requests are considered a second session.
  def isNewSession(col: Column): Column = {
    when(col > inactivityThreshold, lit(1)).otherwise(lit(0))
  }

  // The result DataFrames are written to the local spark-warehouse in parquet files
  def saveAsTable(df: DataFrame, table: String): Unit = {
    df.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(table)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Data Engineer Challenge - Web Log Parser")
      .getOrCreate()

    val logLinesRdd = spark.sparkContext.textFile("data/2015_07_22_mktplace_shop_web_log_sample.log.gz")

    val parsedLogLinesRdd = logLinesRdd.map(parseLogLine)

    import spark.implicits._
    val logDf = parsedLogLinesRdd.toDF

    // Here we filter any rows that are not valid requests according to the details found at
    // https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format
    val filteredLogDf = logDf.filter($"backend_ip".notEqual("-")
      && $"request_processing_time".notEqual("-1")
      && $"backend_processing_time".notEqual("-1")
      && $"response_processing_time".notEqual("-1"))
      // Keeping only the useful fields to answer the given problem
      .select("timestamp", "client_ip", "request", "user_agent")

    // Defining the window that will allow us to determine the sessions
    // BONUS: Because IP addresses do not guarantee distinct users (a family in a household share the same ip address for
    // instance) we could consider adding the user_agent as a grouping key to distinguish users more precisely.
    val windowSpec = Window.partitionBy($"client_ip").orderBy("epoch_seconds")

    // Adding the converted timestamp in seconds so we can make some calculations over other dates
    val logDfWithDuration = filteredLogDf
      .withColumn("epoch_seconds", $"timestamp".cast("timestamp").cast("long"))
      // Calculating the duration since last request by subtracting the time of the current row with the time of the previous row
      .withColumn("duration_seconds", $"epoch_seconds" - lag("epoch_seconds", 1).over(windowSpec).cast("long"))

    // The above operation generates nulls due to each group's first value in the window so we fill them with 0.
    val cleanedLogDfWithDuration = logDfWithDuration.na.fill(0, Array("duration_seconds"))


    val logDfWithSessionId = cleanedLogDfWithDuration
      // Using a helper function we determine if the current request belongs to the same session as the previous one by
      // checking if the duration since last hit is over the session inactivity threshold
      .withColumn("new_session_flag", isNewSession($"duration_seconds"))
      // By summing the new_session_flag over the window we can create a unique index per window
      .withColumn("window_index", sum("new_session_flag").over(windowSpec).cast("string"))
      // Using the created window_index, we create a session_id by concatenating it with the client_ip
      .withColumn("session_id", concat($"client_ip", lit("_"), $"window_index"))
      .cache()

    // ****** 1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    val sessionizedLogDf = logDfWithSessionId.select("client_ip", "session_id", "request", "duration_seconds")
      // We aggregate all the hits in a set and group the data by IP and session
      .groupBy($"client_ip", $"session_id")
      .agg(collect_list($"request").as("requests"), sum($"duration_seconds").as("session_duration_seconds"))

    saveAsTable(sessionizedLogDf, "task1")

    // ****** 2. Determine the average session time
    // The sessions with one hit will in inevitably decrease the average session time (session duration of 0) - but we
    // don't have a better way of determining the actual session time of a user with the given data
    val avgSessionTimeDf = sessionizedLogDf.select(mean("session_duration_seconds").as("average_session_duration"))
    saveAsTable(avgSessionTimeDf, "task2")

    // ****** 3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    // I decided to extract the url from the request and not take into account the request method and HTTP version.
    // Then to only keep the unique URL visits in a session I aggregated the urls into a set and grouped the row by user
    // IP and session id. I added the number of unique URLs visited in a session per visitors
    val uniqueUrlVisitsDf = logDfWithSessionId.withColumn("url", split($"request", " ").getItem(1))
      .select($"client_ip", $"session_id", $"url")
      .groupBy($"client_ip", $"session_id")
      .agg(collect_set($"url").as("unique_url"))
      .withColumn("unique_url_hits_count", size($"unique_url"))

    saveAsTable(uniqueUrlVisitsDf, "task3")

    // ****** 4. Find the most engaged users, ie the IPs with the longest session times
    // To rank the most engaged users, I decided to consider the user's session duration i.e. how long a user spent on
    // your site in total and not the average time on page.
    val mostEngagedUsersDf = sessionizedLogDf.select("client_ip", "session_duration_seconds")
      .groupBy($"client_ip")
      .agg(sum("session_duration_seconds").as("session_duration_total"))
      .sort($"session_duration_total".desc)

    saveAsTable(mostEngagedUsersDf, "task4")
  }
}
