import org.apache.spark.sql.{DataFrame, SparkSession}


object main extends  App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("GDA")
    .getOrCreate();

  val extractDf: DataFrame = extract()

  val transformDf: DataFrame = transform(extractDf)


  def extract(): DataFrame = {
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load("src/main/resources/Final Keywords_2024.csv")
    df.show()
    return df
  }

  def transform(df: DataFrame): DataFrame = {
    df
  }
}

