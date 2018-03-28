import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.io.{File,PrintWriter}

object Task1 {
  def main(args: Array[String]): Unit = {

    Common.checkArgs(args, 3, "HW2 <inputFile> <outputFile> <outputLog>")

    val spark = SparkSession.builder.appName("HW2").getOrCreate()

    //Output Result
    val writer = new PrintWriter(args(2))
    Common.printSpark(writer, spark)

    spark.stop()
    writer.close()

  }
}


object Task2 {
  def main(args: Array[String]): Unit = {

  }
}

object Common{
  def checkArgs(args: Array[String], requiredArgs: Int, style: String): Unit = {
    if (args.length < requiredArgs) {
      System.err.println(s"\nThis program expects $requiredArgs arguments.")
      System.err.println(s"Usage: $style\n")
      System.exit(1)
    }
  }
  def printSpark(writer: PrintWriter, spark: SparkSession): Unit = {
    writer.println("Spark Entity:       " + spark)
    writer.println("Spark version:      " + spark.version)
    writer.println("Spark master:       " + spark.sparkContext.master)
    writer.println("Running 'locally'?: " + spark.sparkContext.isLocal)
    writer.println("")
  }
}
