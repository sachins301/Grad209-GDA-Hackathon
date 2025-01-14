import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, desc, rank, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object PracticeSpark2 extends App{
  val spark = SparkSession.builder()
    .appName("PracticeSpark2")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.conf.set("spark.default.parallelism", 200)
  spark.conf.set("spark.sql.shuffle.partition", 200)
  import spark.implicits._
//  val df = Seq(
//    ("Ashok",100,28,45000),
//    ("Ashok",100,29,49000),
//    ("Rahul",100,25,59000),
//    ("Rahul",100,29,41000),
//    ("Aksha",100,29,99000),
//    ("Ashok",101,39,155000),
//    ("Vivek",101,36,145000),
//    ("Vivek",101,46,195000),
//    ("Arnab",101,56,255000)
//  ).toDF("Name", "Score", "Age", "Salary")

  val data = Seq(
    Row("Ashok",100,28,45000),
    Row("Ashok",100,29,49000),
    Row("Rahul",100,25,59000),
    Row("Rahul",100,29,41000),
    Row("Aksha",100,29,99000),
    Row("Ashok",101,39,155000),
    Row("Vivek",101,36,145000),
    Row("Vivek",101,46,195000),
    Row("Arnab",101,56,255000),
    Row("Arnab", 100, 56, 250),
    Row("Arnab", 100, 21, 401)
  )
  val schema = StructType(Seq(
    StructField("Name", StringType, false),
    StructField("Dept", IntegerType, false),
    StructField("Age", IntegerType, true),
    StructField("Salary", IntegerType, true)
  ))
  val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  df.show()

  //Find the count of duplicate names per department.
  df.groupBy("Name", "Dept").count().filter($"count" > 1)
    .groupBy("Dept")
    .agg(count("*").as("Duplicatename"))
    .show()

  //Find the 4th highest salary in each department.
  df.withColumn("SalaryRank",
    rank().over(Window.partitionBy("Dept").orderBy(desc("Salary"))))
    .filter($"SalaryRank" === 4)
    .show()

  //Find the average age in each department.
  df.groupBy("Dept").agg(avg("Age").alias("AverageAge")).show()
  //Across orginal data
  df.withColumn("AverageDeptAge", avg("Age").over(Window.partitionBy("Dept"))).show()

  df.groupBy("Dept").agg(avg("Age").as("AverageAge")).show()

  println(s"Average age: ${df.select(avg("Age")).first().getDouble(0)}")


  val employeeData = Seq(
    (1, "Alice", 30, "IT", 85000, "2018-06-12", "New York"),
    (2, "Bob", 35, "HR", 65000, "2019-01-20", "San Francisco"),
    (3, "Charlie", 28, "IT", 95000, "2021-03-15", "New York"),
    (4, "David", 40, "Finance", 105000, "2015-09-01", "Seattle"),
    (5, "Eve", 25, "IT", 75000, "2022-07-22", "San Francisco"),
    (6, "Frank", 38, "HR", 55000, "2017-11-13", "New York")
  )

  // Create DataFrame
  val employeeColumns = Seq("EmployeeID", "Name", "Age", "Department", "Salary", "JoiningDate", "City")
  val employeeDF = employeeData.toDF(employeeColumns: _*)

  // Find the average salary of employees in each department.
  employeeDF.groupBy("Department").agg(avg("Salary").alias("AverageSalary")).show()

  // List all employees who joined after January 1, 2020.
  employeeDF.filter($"JoiningDate" > "2020-01-01").show()

  //Find the department with the highest average salary.
  employeeDF.groupBy("Department").agg(avg("Salary").alias("AverageSalary"))
    .select("Department", "AverageSalary")
    .orderBy(desc("AverageSalary"))
    .limit(1)
    .show()

  //Count the number of employees in each city.
  employeeDF.groupBy("City").count().show()

  //Calculate the average age of employees in the "IT" department.
  employeeDF.filter($"Department" === "IT")
    .select(avg("Age").as("AverageAge"))
    .show()


  val salesData = Seq(
    (1001, 1, "P101", "Electronics", 1, 500, "2022-01-01"),
    (1002, 2, "P102", "Apparel", 3, 150, "2022-01-02"),
    (1003, 1, "P103", "Electronics", 2, 300, "2022-01-03"),
    (1004, 3, "P104", "Home & Living", 1, 700, "2022-01-05"),
    (1005, 4, "P105", "Electronics", 5, 200, "2022-01-06"),
    (1006, 2, "P106", "Apparel", 2, 100, "2022-01-07")
  )

  // Create DataFrame
  val salesColumns = Seq("TransactionID", "CustomerID", "ProductID", "Category", "Quantity", "Price", "TransactionDate")
  val salesDF = salesData.toDF(salesColumns: _*)

  // Calculate the total revenue generated for each product category.
  salesDF
    .withColumn("Revenue", col("Quantity") * col("Price"))
    .groupBy("Category").agg(sum("Revenue").as("TotalRevenue"))
    .show()

  // Identify the customer who made the highest number of purchases.
  salesDF.groupBy("CustomerID").agg(count("*").alias("count")).orderBy(desc("count")).limit(1).show()

  //Find the average revenue per transaction.
  salesDF.withColumn("Revenue", col("Quantity") * col("Price"))
    .select(avg("Revenue").as("AverageRevenue"))
    .show()

  // Determine the most popular product category by the number of transactions.
  salesDF.groupBy("Category").count().orderBy(desc("count")).limit(1).show()

  //Get the list of all transactions where the revenue exceeds $500.
  salesDF.withColumn("Revenue", col("Quantity") * col("Price"))
    .filter($"Revenue" > 500)
    .show()


  val salesDf = Seq(
    ("2023-01-01", "Electronics", "Laptop", 2, 1000, "C001"),
    ("2023-01-02", "Electronics", "Smartphone", 5, 500, "C002"),
    ("2023-01-03", "Furniture", "Chair", 10, 50, "C003"),
    ("2023-01-04", "Furniture", "Table", 3, 150, "C004"),
    ("2023-01-05", "Clothing", "T-Shirt", 20, 20, "C005"),
    ("2023-01-06", "Clothing", "Jeans", 15, 40, "C006"),
    ("2023-01-07", "Electronics", "Smartphone", 2, 500, "C007"),
    ("2023-01-08", "Furniture", "Chair", 4, 50, "C008")
  ).toDF("Date", "Category", "Product", "Quantity", "Price", "CustomerID")
    .withColumn("Revenue", col("Quantity") * col("Price"))

  // Find all transactions for the "Electronics" category.
  salesDf.filter($"Category" === "Electronics").show()

  // Calculate the total revenue generated per category.
  salesDf.groupBy("Category").agg(sum("Revenue").alias("TotalRevenue")).show()

  // Find the total number of products sold in each category.
  salesDf.groupBy("Category").agg(countDistinct("Product")).show()

  val customerDf = Seq(
    ("C001", "Alice", "New York"),
    ("C002", "Bob", "Los Angeles"),
    ("C003", "Charlie", "Chicago"),
    ("C004", "David", "Houston"),
    ("C005", "Eve", "San Francisco"),
    ("C006", "Frank", "Seattle"),
    ("C007", "Grace", "Miami"),
    ("C008", "Hank", "Denver")
  ).toDF("CustomerID", "CustomerName", "City")

  // Join salesData with customerData to get the city for each transaction.
  val salesCityDf = salesDf.join(customerDf, Seq("CustomerID"), "left_outer")

  // Find the total revenue generated by customers from "New York".
  salesCityDf.groupBy("City").agg(sum("Revenue").as("TotalRevenue")).show()

  // For each category, calculate the rank of products by revenue (highest first).
  salesCityDf.groupBy("Category", "Product").agg(sum("Revenue").as("TotalRevenue"))
    .withColumn("Rank",
      rank().over(Window.partitionBy("Category").orderBy(desc("TotalRevenue"))))
    .show()

  // Find the cumulative revenue generated by each product category.



  scala.io.StdIn.readLine("Waiting . . . ")
}
