import org.apache.spark.sql.SparkSession
import java.io.{File,PrintWriter}

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/mnt/c/home/school/bdm/spark/test.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}

object Task{
  def main(args: Array[String]){
    val writer = new PrintWriter(new File("test.txt"))
    writer.println("Hi")
    Console.println("hi")
    writer.close()
  }
}
