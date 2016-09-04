package com.examples

import scala.collection.mutable.ListBuffer
import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * A customer
 */
case class Customer (customerId : String, // Unique customer identifier
                     name : String, // Customer Name
                     dob : String) // Customer date of birth

/**
 * A transaction
 */
case class Transaction (transactionId : String, // Unique transaction identifier
                        customerId : String, // Unique customer identifier
                        transactionValueGbpInPence : Int, // Value of transaction in GBP, pence, e.g. 300 is £3.00
                        items : Int) // Number of items bought in transaction

/**
 * Promotions a customer triggered on transaction
 */
case class Promotion (transactionId : String, // Unique transaction identifier
                      promotionId : String, // Unique promotion identifier
                      promotionValueGbpInPence : Int) // Value of the promotion triggered

/**
 * Products bought as part of a transaction
 */
case class Line (transactionId : String, // Unique transaction identifier
                 productId : String, // Unique product identifier
                 quantity : Int, // Number of product bought
                 priceGbpInPence : Int, // Price per product in GBP, pence, e.g. 300 is £3.00
                 amountGbpInPence : Int) // Total amount paid for product e.g. quantity * priceGbpInPence

/**
 * Transformed transactional data that will be using for analytics purposes
 */
case class Sale (customerId : String, // Unique customer identifier
                 yearOfBirth : String, // Customer year of birth e.g 1980
                 transactionId : String, // Unique transaction identifier
                 transactionValueGbp : Double, // Value of transaction in GBP e.g. £3.00
                 items : Int, // Number of items in purchased
                 numberPromotions : Int, // Total number of promotions triggered
                 promotionTotalValueInGbp : Double, // Total value of all the promotions
                 averageItemValueInGbp : Double) // Average value of the items purchased


/**
 * Data Engineer Test
 *
 * This job will read the input files and transform the data to the desired output format (Sale case class) and write the results out
 */
object SalesTransform {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    val sc = new SparkContext(conf)

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create an RDD of Person objects and register it as a table.
    val customers = sc.textFile("src/main/resources/customer.tsv").map(_.split("\t")).map(c => Customer(c(0), c(1), c(2))).toDF()
    customers.registerTempTable("customer")
    println("Cutomer Details");
    customers.show();

    val transactions = sc.textFile("src/main/resources/transaction.tsv").map(_.split("\t")).map(t => Transaction(t(0), t(1), t(2).toInt, t(3).toInt)).toDF()
    transactions.registerTempTable("transaction")
    println("Transaction Details");
    transactions.show();

    val promotions = sc.textFile("src/main/resources/promotion.tsv").map(_.split("\t")).map(p => Promotion(p(0), p(1), p(2).toInt)).toDF()
    promotions.registerTempTable("promotion")
    println("Promotion Details");
    promotions.show();

    val lines = sc.textFile("src/main/resources/line.tsv").map(_.split("\t")).map(l => Line(l(0), l(1), l(2).toInt, l(3).toInt, l(4).toInt)).toDF()
    lines.registerTempTable("line")
    println("Line Details")
    lines.show()

    val sales = sc.textFile("src/main/resources/sale.tsv").map(_.split("\t")).map(s => Sale(s(0), s(1), s(2), s(3).toDouble, s(4).toInt, s(5).toInt, s(6).toDouble, s(7).toDouble)).toDF()
    sales.registerTempTable("sale")
    println("Sale Details")
    sales.show()
    
    //write sale data to parquet file
    //sales.drop("sale.parquet")
    //sales.save("sale.parquet", SaveMode.Overwrite)
    sales.write.mode(SaveMode.Overwrite);
    sales.write.parquet("sale.parquet")
    val parquetFile = sqlContext.read.parquet("sale.parquet")

    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("sale")
    val salesDF = sqlContext.sql("SELECT customerId,transactionId FROM sale")
    salesDF.show()

    // ---- Schema Programatically
    // The schema is encoded in a string
    /*val schemaString = "customerId yearOfBirth transactionId transactionValueGbp items numberPromotions promotionTotalValueInGbp averageItemValueInGbp"

    // Import Row.
    import org.apache.spark.sql.Row;

    // Import Spark SQL data types
    import org.apache.spark.sql.types.{ StructType, StructField, StringType };

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (sale) to Rows.
    val rowRDD = sales.map(p => Row(p(0), p(1)))

    // Apply the schema to the RDD.
    val salesDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    salesDataFrame.registerTempTable("sale")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT customerId,transactionId FROM sale")
    println("####### Results ------")
    results.show();*/


  }

}