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
  def getFile(fileString: String): Array[String] ={
    val inputPath = new Path(fileString)
    val inputBuffer = scala.collection.mutable.ArrayBuffer.empty[String]
    val iterator = inputPath.getFileSystem(new Configuration()).listFiles(inputPath, false)
    while(iterator.hasNext()){
        val fileStatus = iterator.next()
        if(fileStatus.isFile()){
          inputBuffer += fileStatus.getPath().toString()
        }
    }
    inputBuffer.toArray
  }

  def outputWriter(fileString: String): PrintWriter ={
    val outputPath = new Path(fileString)
    val outputStream = outputPath.getFileSystem(new Configuration()).create(outputPath);
    new PrintWriter(outputStream)
  }

  def printSample(writer: PrintWriter, data: Any, title: String, format: String){
    writer.println(title+" Data Sample: " + format)
    writer.println(data+"\n")
  }

  def checkArgs(args: Array[String], requiredArgs: Int, style: String): Unit = {
    if (args.length < requiredArgs) {
      System.err.println(s"\nThis program expects $requiredArgs arguments.")
      System.err.println(s"Usage: -- $style\n")
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
