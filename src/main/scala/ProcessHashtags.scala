import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object ProcessHashtags extends App{
  // Create SparkSession
  val spark = SparkSession.builder()
    .appName("Hashtag DataFrame")
    .config("spark.master", "local[*]")
    .getOrCreate()

  // Read CSV file into DataFrame
  val hashtag_df = spark.read.format("csv")
    .option("header", "true")
    .load("src/main/resources/hashtag.csv")

  // Define a function to convert columns to list of hashtags
  def hashtagsToList(row: org.apache.spark.sql.Row): List[String] = {
    println(row)
    val hashtags = for {
      i <- 1 to 30
      col = s"hashtag$i"
      if !row.isNullAt(row.fieldIndex(col)) // Exclude null values
    } yield row.getString(row.fieldIndex(col))
    hashtags.toList
  }

  def hashtagColList(n: Int, list: List[String]): List[String] = {
    if (n < 1) list
    else hashtagColList(n-1, list :+ "hashtag"+n)
  }

  val hashtag_df_select = hashtag_df.select(col("text") +: hashtagColList(30, List()).map(col):_*)


  // Register the function as a UDF (User Defined Function)
  val hashtagsToListUDF = udf((row: org.apache.spark.sql.Row) => hashtagsToList(row))

  // Apply the UDF to create the 'hashtag' column
  val hashtag_df_with_hashtags = hashtag_df_select
    .na.fill("NA").na.replace("*", Map("NA" -> null))
    .withColumn("hashtag", hashtagsToListUDF(struct(hashtag_df_select.columns.map(col): _*)))

  // Show the DataFrame with 'hashtag' column
  hashtag_df_with_hashtags.show()
}
