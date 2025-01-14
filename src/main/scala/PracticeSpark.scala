import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, desc, rank, sum, udf}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.LongAccumulator

object PracticeSpark extends App {
  val spark = SparkSession.builder()
    .appName("SparkPractice")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.conf.set("spark.default.parallelism", 200)
  spark.conf.set("spark.sql.shuffle.partitions", 200)
  spark.sparkContext.setLocalProperty("spark.ui.view.acumulators", "true")


  import spark.implicits._

  val schema = StructType(Array(
    StructField("Id", IntegerType, true),
    StructField("SubmittedUserId", StringType, true),
    StructField("TeamId", StringType, true),
    StructField("SourceKernelVersionId", StringType, true),
    StructField("SubmissionDate", StringType, true),
    StructField("ScoreDate", StringType, true),
    StructField("IsAfterDeadline", StringType, true),
    StructField("PublicScoreLeaderboardDisplay", DoubleType, true),
    StructField("PublicScoreFullPrecision", DoubleType, true),
    StructField("PrivateScoreLeaderboardDisplay", StringType, true),
    StructField("PrivateScoreFullPrecision", StringType, true)
  ))

  val df = spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load("src/main/resources/Submissions.csv")
    .na.fill(Map(
      "PublicScoreFullPrecision" -> 0.0,
      "PublicScoreLeaderboardDisplay" -> 0.0
    )).repartition(200)

//  df.show()
//
//  println("Before partitioning: " + df.rdd.getNumPartitions)
//  val repartitionedDF = df.repartition(15)
//  println("After partitioning: " + repartitionedDF.rdd.getNumPartitions) // Should print 15
//
//  val filteredDf = repartitionedDF.filter(col("IsAfterDeadline") === true).groupBy($"TeamId").count()
//  filteredDf.show()
//
//
//  // Accumulator example
//  val accumulator: LongAccumulator = spark.sparkContext.longAccumulator("DeadlineAccumulator")
//  repartitionedDF
//    .filter(col("IsAfterDeadline") === "True")
//    .foreach(_ => accumulator.add(1))
//  println(s"Total no of submission after deadline: ${accumulator.value}")


  // UDF
  df.printSchema()
  val multiplierUdf: UserDefinedFunction = udf((x: Double) => x * 100)
  val udfdf = df.withColumn("ScorePercent", multiplierUdf(col("PublicScoreLeaderboardDisplay")))
  udfdf.select("Id", "TeamId", "PublicScoreLeaderboardDisplay", "ScorePercent").show()

  // window function
  df.withColumn("TeamScore",
    sum("PublicScoreFullPrecision")
      .over(Window.partitionBy("TeamId"))
    )
    .withColumn("TeamRank",
      rank().over(Window.orderBy("TeamScore"))
    )
    .select("Id", "TeamId", "PublicScoreFullPrecision", "TeamScore", "TeamRank")
    .show()

  //group by
  df.groupBy("TeamId").agg(sum("PublicScoreFullPrecision").alias("TeamScore"))
    .withColumn("TeamRank", rank().over(Window.orderBy(desc("TeamScore"))))
    .show()

  // To prevent spark UI from clossing
  scala.io.StdIn.readLine()
}
