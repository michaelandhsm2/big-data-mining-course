import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

import java.io.{File,PrintWriter}

object Task1 {
  def main(args: Array[String]): Unit = {

    Common.checkArgs(args, 3, "<inputFile(dir or file)> <outputFile(dir)> <outputLog(.txt)>")
    val inputList = Common.getFile(args(0))

    val spark = SparkSession.builder.appName("HW2").getOrCreate()
    var flattenSocialData = spark.sparkContext.emptyRDD[((String, Int), (Double, Int))]

    //Create Initial Tuple
    inputList.foreach{ input =>
        val data = spark.sparkContext.textFile(input)
        val header = data.first
        val flattenData = data.filter(l => l != header).
        flatMap{ dataString =>
            val attr = dataString.split(",")
            attr.zipWithIndex.
            filter{ case (value,index) => index >= 1 }.
            map{ case (value,index) => ((attr(0),(index-1)/3),(value.toDouble,1)) }
        }
        flattenSocialData = flattenSocialData.union(flattenData)
    }
    flattenSocialData.persist()

    // Generate Popularity by Day/Hour
    val pop_by_hour = flattenSocialData.reduceByKey{case ((ia, ib), (ja, jb)) => (ia+ja, ib+jb)}

    val pop_by_day = flattenSocialData.
        map{case((uid, hr), (sum, count)) => ((uid, hr/24), (sum/count, 1))}.
        reduceByKey{case ((ia, ib), (ja, jb)) => (ia+ja, ib+jb)}.
        map{case((uid, day), (sum, count)) => (uid,("Day", day, sum / count))}

    val all_pop = pop_by_hour.
        map{case((uid, hr), (sum, count)) => (uid,("Hour", hr, sum / count))}.
        union(pop_by_day).groupByKey.sortByKey(ascending = false)

    //Consentrate populatarity of each news, and transform to csv
    val all_tuple = all_pop.map { case(uid, iterable) =>
        val vect = iterable.toVector.sortBy { tup =>
            (tup._1, tup._2)
        }.map{ case (doh, no, value) => value }
        uid+","+vect.mkString(",")
    }
    all_tuple.saveAsTextFile(args(1))

    //Output Result
    val writer = Common.outputWriter(args(2))
    Common.printSpark(writer, spark)

    writer.println("Loaded Files:")
    inputList.foreach(writer.println)
    writer.println("")

    def printSample(data:Any, title:String, format:String){
      writer.println(title+" Data Sample: " + format)
      writer.println(data+"\n")
    }

    printSample(flattenSocialData.first(), "Loaded", "((UID, Hour), (Popularity, Count))")
    printSample(pop_by_hour.first(), "Intermediate", "((UID, Hour), (Popularity, Count))")
    printSample(pop_by_day.first(), "Popularity by Day", "((UID, ('Day', No., Popularity Average))")
    printSample(all_pop.first(), "All Popularity", "(UIN, ComactBuffer(all tuples))")

    val test_tuple = all_pop.first()
    printSample(all_pop.first()._2.toVector.sortBy { tup => (tup._1, tup._2) },
                "Sorted Values", "Vector - Day * 2, Hour * 48")
    printSample(all_tuple.first(), "Finalized", "UID, Day 1, Day 2, Hour 1, Hour 2 ... Hour 48")

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