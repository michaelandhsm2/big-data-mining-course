import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
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
    val inputPath = new Path(args(0));
    val fs = inputPath.getFileSystem(new Configuration())
    val iterator = fs.listFiles(inputPath, true);
    if(fs.isDirectory(inputPath)){
        println("Is Dir")
    }else if(fs.isFile(inputPath)){
        println("Is file")
    }

    while(iterator.hasNext()){
        val fileStatus = iterator.next();
        println(fileStatus.getPath().toString())
    }
  }
}
