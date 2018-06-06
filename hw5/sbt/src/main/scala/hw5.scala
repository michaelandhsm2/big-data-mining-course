import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

import java.io.{File,PrintWriter}

object Task {
  def main(args: Array[String]): Unit = {

    Common.checkArgs(args, 3, "HW5 <inputFile> <outputFile> <outputLog>")

    val spark = SparkSession.builder.appName("HW5").getOrCreate()
    val data = spark.sparkContext.textFile(args(0))

    // Remove Header
    val rows = data.filter(l => l.charAt(0) != '#')
    val flattenData = rows.
        flatMap{ dataString =>
            dataString.split("\\s+").
                zipWithIndex.
                map{
                    case (value,index) =>
                        index match{
                            case 0 => (value, (0, 1))
                            case 1 => (value, (1, 0))
                        }
                    }
        }.reduceByKey{ case((in1, out1), (in2, out2)) => (in1+in2, out1+out2)}

    val out = flattenData.sortBy{case(id, (in, out)) => -out}.map{case(id, (in, out)) => id+", "+out}
    out.saveAsTextFile(args(1)+"/outDegree")

    val in = flattenData.sortBy{case(id, (in, out)) => -in}.map{case(id, (in, out)) => id+", "+in}
    in.saveAsTextFile(args(1)+"/inDegree")


    //Output Result
    val writer =  Common.outputWriter(args(2))
    Common.printSpark(writer, spark)
    writer.println("Count: "+rows.count)

    spark.stop()
    writer.close()

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
