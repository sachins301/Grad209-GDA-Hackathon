import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.util.LongAccumulator

object PracticeSpark extends App {
  val spark = SparkSession.builder()
    .appName("SparkPractice")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.conf.set("spark.default.parallelism", 15)
  spark.conf.set("spark.sql.shuffle.partitions", 15)
  spark.sparkContext.setLocalProperty("spark.ui.view.acumulators", "true")


  import spark.implicits._

  val df = spark.read.format("csv")
    .option("header", "true")
    .load("src/main/resources/Submissions.csv")

  df.show()

  println("Before partitioning: " + df.rdd.getNumPartitions)
  val repartitionedDF = df.repartition(15)
  println("After partitioning: " + repartitionedDF.rdd.getNumPartitions) // Should print 15

  val filteredDf = repartitionedDF.filter(col("IsAfterDeadline") === true).groupBy($"TeamId").count()
  filteredDf.show()


  // Accumulator example
  val accumulator: LongAccumulator = spark.sparkContext.longAccumulator("DeadlineAccumulator")
  repartitionedDF
    .filter(col("IsAfterDeadline") === "True")
    .foreach(_ => accumulator.add(1))
  println(s"Total no of submission after deadline: ${accumulator.value}")



  // To prevent spark UI from clossing
  scala.io.StdIn.readLine()
}
