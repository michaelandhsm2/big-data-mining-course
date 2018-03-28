import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.io.File

object Task1 {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: HW1 <inputFile> <outputFile>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("HW2")
      .getOrCreate()

    spark.stop()

  }
}


object Task2 {
  def main(args: Array[String]): Unit = {

  }
}
