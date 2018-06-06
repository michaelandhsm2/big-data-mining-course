import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ListBuffer
import java.io.{File,PrintWriter}

object Task {
  def main(args: Array[String]): Unit = {

    Common.checkArgs(args, 3, "Bonus <inputFile> <outputFile> <outputLog>")

    val spark = SparkSession.builder.appName("Bonus").getOrCreate()

    val data = spark.sparkContext.textFile(args(0))

    val header = data.first()
    val rows = data.filter(l => l!=header)

    val regex = "\\d{2}\\/\\d{2}\\/\\d{4}\\s(\\d{2}):\\d{2}:\\d{2}\\s(\\wM)".r
    val regex2 = "(?:^\\d{2,5}X{0,3}\\s*\\w*\\s*)*([\\w ]+)".r
    val flattenData = rows.
        map{ dataString =>
            val tuple = dataString.split(",").toSeq
            var list =  ListBuffer[String]()
            var list2 =  ListBuffer[String]()
            for(m <- regex.findAllIn(tuple(2)).matchData;
              e <- m.subgroups)
              if(e!=null) list+=e
            for(m <- regex2.findAllIn(tuple(3)).matchData;
              e <- m.subgroups)
              if(e!=null) list2+=e
            (tuple(5), tuple(7), list2.toSeq.mkString(""), list.toSeq.mkString(""))
        }

    val primeCount = flattenData.map(tuple => (tuple._1, 1)).reduceByKey{(i, j) => i+j}.sortBy{case(prim, count) => -count}
    primeCount.saveAsTextFile(args(1)+"/task1/primeCount")

    val locCount = flattenData.map(tuple => (tuple._2, 1)).reduceByKey{(i, j) => i+j}.sortBy{case(loc, count) => -count}
    primeCount.saveAsTextFile(args(1)+"/task1/locCount")

    val locGroup = flattenData.map(tuple => ((tuple._2, tuple._1), 1)).
                    reduceByKey{(i, j) => i+j}.sortBy{case((loc, prime), count) => -count}.
                    map{
                      tuple => (tuple._1._1, (tuple._1._2, tuple._2))
                    }.groupByKey

    locGroup.foreach{
        case(loc, vect) =>
        var newLoc = loc.split("\\W+").mkString("_")
        val locGroupWriter = Common.outputWriter(args(1)+"/task2/"+newLoc+".csv")
        vect.foreach{
            vectString => locGroupWriter.println(vectString)
        }
        locGroupWriter.close()
    }

    val primeGroup = flattenData.map{
                        tuple =>
                        ( ( tuple._1, tuple._3 ), 1)
                    }.
                    reduceByKey{(i, j) => i+j}.sortBy{case((prime, loc), count) => -count}.
                    map{
                      tuple => (tuple._1._1, (tuple._1._2, tuple._2))
                    }.groupByKey

    primeGroup.foreach{
        case(prime, vect) =>
        var newPrime = prime.split("\\W+").mkString("_")
        val primeGroupWriter = Common.outputWriter(args(1)+"/task3/"+newPrime+".csv")
        vect.foreach{
            vectString => primeGroupWriter.println(vectString)
        }
        primeGroupWriter.close()
    }

    val primeHours = flattenData.map{
                        tuple =>
                        ( ( tuple._1,  tuple._4), 1)
                    }.
                    reduceByKey{(i, j) => i+j}.sortBy{case((prime, hours), count) => -count}.
                    map{
                      tuple => (tuple._1._1, (tuple._1._2, tuple._2))
                    }.groupByKey

    primeHours.foreach{
        case(prime, vect) =>
        var newPrime = prime.split("\\W+").mkString("_")
        val primeHoursWriter = Common.outputWriter(args(1)+"/task4-prime/"+newPrime+".csv")
        vect.foreach{
            vectString => primeHoursWriter.println(vectString)
        }
        primeHoursWriter.close()
    }

    val locHours = flattenData.map{
                        tuple =>
                        ( ( tuple._2,  tuple._4), 1)
                    }.
                    reduceByKey{(i, j) => i+j}.sortBy{case((loc, hours), count) => -count}.
                    map{
                      tuple => (tuple._1._1, (tuple._1._2, tuple._2))
                    }.groupByKey

    locHours.foreach{
        case(loc, vect) =>
        var newLoc = loc.split("\\W+").mkString("_")
        val locHoursWriter = Common.outputWriter(args(1)+"/task4-loc/"+newLoc+".csv")
        vect.foreach{
            vectString => locHoursWriter.println(vectString)
        }
        locHoursWriter.close()
    }


    //Output Result
    val writer =  Common.outputWriter(args(2))
    Common.printSpark(writer, spark)

    Common.printSample(writer, rows.first(), "Input Data Sample", "")
    writer.println("\nCount: "+rows.count+"\n")
    Common.printSample(writer, flattenData.take(10).mkString("\n"), "Flatten Data Sample", "( Primary Type, Location Description, Street, Date)\n")
    Common.printSample(writer, primeCount.take(5).mkString("\n"), "Primary Count Sample", "( Primary Type, Count)")
    Common.printSample(writer, locCount.take(5).mkString("\n"), "Location Count Sample", "( Location Description, Count)\n")
    Common.printSample(writer, locGroup.take(2).mkString("\n"), "Location Group Sample", "( Location Description, [(Primary Type, Count)])\n")
    Common.printSample(writer, primeGroup.take(2).mkString("\n"), "Primary Group Sample", "( Primary Type, [(Street, Count)])\n")
    Common.printSample(writer, primeHours.take(2).mkString("\n"), "Primary Hours Sample", "( Primary Type, [(Hours, Count)])\n")
    Common.printSample(writer, locHours.take(2).mkString("\n"), "Location Hours Sample", "( Location Type, [(Hours, Count)])\n")

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
