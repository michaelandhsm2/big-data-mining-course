import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ListBuffer
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

    Common.printSample(writer,flattenSocialData.first(), "Loaded", "((UID, Hour), (Popularity, Count))")
    Common.printSample(writer,pop_by_hour.first(), "Intermediate", "((UID, Hour), (Popularity, Count))")
    Common.printSample(writer,pop_by_day.first(), "Popularity by Day", "((UID, ('Day', No., Popularity Average))")
    Common.printSample(writer,all_pop.first(), "All Popularity", "(UIN, ComactBuffer(all tuples))")

    val test_tuple = all_pop.first()
    Common.printSample(writer,all_pop.first()._2.toVector.sortBy { tup => (tup._1, tup._2) },
                "Sorted Values", "Vector - Day * 2, Hour * 48")
    Common.printSample(writer,all_tuple.first(), "Finalized", "UID, Day 1, Day 2, Hour 1, Hour 2 ... Hour 48")

    spark.stop()
    writer.close()
  }
}


object Task2 {
  def main(args: Array[String]): Unit = {
    Common.checkArgs(args, 2, "<inputFile(.csv)> <outputLog(.txt)>")

    val spark = SparkSession.builder.appName("HW2").getOrCreate()

    // Load File
    import spark.implicits._
    val newsData_string = spark.sparkContext.textFile(args(0))

    // Remove Header
    val header = newsData_string.first()
    var regex = """,([\d.-]+)$|^([\de+.-]+)(?=,)|(?:,)([\d.-]+)(?=,)|(?:,"+)((?:[^"]+"{3,})*[^"]*)(?="+,)""".r
    var newsParse = newsData_string.filter(x => x!=header).map{
        string =>
        var list =  ListBuffer[String]()
        for(m <- regex.findAllIn(string).matchData;
          e <- m.subgroups)
          if(e!=null) list+=e
        list.toSeq
    }

    val flattenSentientScore = newsParse.map{
        attr =>
        (attr(4),(attr(6).toString.toDouble, attr(7).toString.toDouble, 1))
    }

    var reducedSentientScore = flattenSentientScore.reduceByKey(
        (a,b) =>
        (a._1 + b._1, a._2+ b._2, a._3 + b._3)
    )
    var finalSentientScore = reducedSentientScore.map{
        case (topic,(titleSum, headSum, count)) =>
        (topic, titleSum, headSum, titleSum/count, headSum/count, count)
    }

    //Output Result
    val writer = Common.outputWriter(args(1))
    Common.printSpark(writer, spark)

    Common.printSample(writer,newsParse.first(), "Parse", "")
    Common.printSample(writer,flattenSentientScore.first(), "Flatten Sentient Score", "(Topic, (Title Sentient Score, Headline Sentient Score, Count))")
    Common.printSample(writer,reducedSentientScore.first(), "Reduced Sentient Score", "(Topic, (Title Sentient Score, Headline Sentient Score, Count))")

    def myprint(s: Tuple6[Any, Double, Double, Double, Double, Any]): Unit = {
            writer.println("For "+s._1+" ("+s._6+" entries): ")
            writer.println("        Sum of Title Sentient Score: " + s._2)
            writer.println("        Sum of Headline Sentient Score: " + s._3)
            writer.println("        Average of Title Sentient Score: " + s._4)
            writer.println("        Average of Headline Sentient Score: " + s._5 + "\n")
        }

    println("\nFinal Sentient Score: \n")
    finalSentientScore.collect().foreach(myprint)

    spark.stop()
    writer.close()
  }
}

object Task3_4 {
  def main(args: Array[String]): Unit = {
    Common.checkArgs(args, 3, "<inputFile(.csv)> <outputFile(folder)> <outputLog(.txt)>")

    val spark = SparkSession.builder.appName("HW2").getOrCreate()

    // Load File
    import spark.implicits._
    val newsData_string = spark.sparkContext.textFile(args(0))

    // Remove Header
    val header = newsData_string.first()
    var regex = """,([\d.-]+)$|^([\d.e+-]+)(?=,)|(?:,)([\d.-]+)(?=,)|(?:,"+)((?:[^"]+"{3,})*[^"]*)(?="+,)""".r
    var newsParse = newsData_string.filter(x => x!=header).map{
        string =>
        var list =  ListBuffer[String]()
        for(m <- regex.findAllIn(string).matchData;
          e <- m.subgroups)
          if(e!=null) list+=e
        list.toSeq
    }

    def wordPreProcess(input: Any): Array[String] = {
            var matchRegex = """([$]?(?:[\w]+(?:[\w',]*[\w]+)+|[\w]))""".r
            var list =  ListBuffer[String]()
            for(m <- matchRegex.findAllIn(input.toString.toLowerCase).matchData;
              e <- m.subgroups)
              list+=e
            list.toArray
        }

    // Task 3
    var flattenWordTuples = newsParse.
      flatMap{ attr =>
          var titleWords = wordPreProcess(attr(1)).map(
              word =>
              (word, "title", attr(4).toString, attr(5).toString.split("\\s")(0))
          )

          var headlineWords = wordPreProcess(attr(2)).map(
              word =>
              (word, "headine", attr(4).toString, attr(5).toString.split("\\s")(0))
          )

          titleWords ++ headlineWords
      }

    flattenWordTuples.persist()

    var perTopicTuple = flattenWordTuples.
      map{ case (word, toh, topic, date) =>
        ((word, toh, topic), 1)
      }.reduceByKey{
          (j, k) =>
          j+k
      }
    var perTopicOutput = perTopicTuple.
      map{ case ((word, toh, topic), count) =>
        ((toh, topic), (word, count))
      }.groupByKey.mapValues{ iterator =>
          iterator.toVector.sortBy { case(word, count) =>
              (-count, word)
          }.map{
              case(word,count) => word + ","+ count
          }.mkString("\n")
      }
    perTopicOutput.foreach{ case ((toh, topic), vect_string) =>
      val local_writer =  Common.outputWriter(args(1)+"/topic/"+topic+"_"+toh+".csv")
      try {local_writer.write(vect_string)} finally {local_writer.close()}
    }

    var perDayTuple = flattenWordTuples.
      map{ case (word, toh, topic, date) =>
        ((word, toh, date), 1)
      }.reduceByKey{
          (j, k) =>
          j+k
      }
    var perDayOutput = perDayTuple.
      map{ case ((word, toh, date), count) =>
        ((toh, date), (word, count))
      }.groupByKey.mapValues{ iterator =>
          iterator.toVector.sortBy { case(word, count) =>
              (-count, word)
          }.map{
              case(word,count) => word + ","+ count
          }.mkString("\n")
      }
    perDayOutput.foreach{ case ((toh, date), vect_string) =>
      val local_writer =  Common.outputWriter(args(1)+"/date/"+date+"_"+toh+".csv")
      try {local_writer.write(vect_string)} finally {local_writer.close()}
    }

    var totalTuple = perTopicTuple.
      map{ case ((word, toh, topic), count) =>
        ((word, toh), 1)
      }.reduceByKey{
          (j, k) =>
          j+k
      }
    var totalOutput = totalTuple.
      map{ case ((word, toh), count) =>
        (toh, (word, count))
      }.groupByKey.mapValues{ iterator =>
          iterator.toVector.sortBy { case(word, count) =>
              (-count, word)
          }.map{
              case(word,count) => word + ","+ count
          }.mkString("\n")
      }
    totalOutput.foreach{ case (toh, vect_string) =>
      val local_writer =  Common.outputWriter(args(1)+"/total/total_"+toh+".csv")
      try {local_writer.write(vect_string)} finally {local_writer.close()}
    }

    //Task 4
    val sizeOfTop = 100
    val perTopicTop100 = perTopicTuple.
      map{ case ((word, toh, topic), count) =>
        ((word, topic), count)
      }.reduceByKey{
          case (i, j) => i + j
      }.
      map{ case ((word, topic), count) =>
        (topic, (word, count))
      }.groupByKey.mapValues{ iterator =>
          iterator.toVector.sortBy { case(word, count) =>
              (-count, word)
          }.take(sizeOfTop).map{case(word, count) => word}
      }.collectAsMap()

    var perTopicOccurance = newsParse.
      map{ attr =>
          var titleAndHeadline = wordPreProcess(attr(1).toString + attr(2).toString)
          var list =  ListBuffer[Int]()
          perTopicTop100(attr(4)).foreach{ word =>
              list += (if (titleAndHeadline.contains(word)) 1 else 0)
          }
          (attr(4),list.toSeq)
      }.groupByKey
    var exportMatrix = perTopicOccurance.map{
          case (topic, iterator) =>
          val matrix = Array.ofDim[Int](sizeOfTop,sizeOfTop)
          val occur_matrix = Array.ofDim[Int](sizeOfTop,sizeOfTop)
          val result_matrix = Array.ofDim[Double](sizeOfTop,sizeOfTop)
          iterator.map{
              list =>
              val indexWithValue = list.zipWithIndex.filter(_._1 != 0).map(_._2)
              for( x <- 0 until indexWithValue.length ; y <- 0 until indexWithValue.length ){
                  matrix(indexWithValue(x))(indexWithValue(y)) += 1
              }

          }
          val resultString = ","+perTopicTop100(topic).mkString(",") +"\n" +
                             matrix.zipWithIndex.map{ case(x,i) => perTopicTop100(topic)(i)+
                             ","+x.mkString(",")}.mkString("\n")
          (topic,resultString)
      }

    exportMatrix.foreach{
        case (topic, resultString) =>
        val local_writer =  Common.outputWriter(args(1)+"/co-occurance/"+topic+".csv")
        try {local_writer.write(resultString)} finally {local_writer.close()}
    }

    //Output Result
    val writer = Common.outputWriter(args(2))
    Common.printSpark(writer, spark)

    def printWrittenFiles(s: String): Unit = {
        val inputList = Common.getFile(args(1)+"/"+s)
        writer.println("Created In Folder "+ s +": ")
        inputList.foreach{ x =>
          writer.print(x + ", ")
        }
        writer.println("\n\n")
    }

    printWrittenFiles("topic")
    printWrittenFiles("date")
    printWrittenFiles("total")
    printWrittenFiles("co-occurance")

    writer.println("\n\nNormalize Output Examples printed on Console View.")
    writer.close()

    var df_2 = spark.read.
      format("csv").
      option("header", "true").
      csv(args(1)+"/co-occurance/"+perTopicOccurance.first()._1+".csv")

    println("\n\nCo-Occurance Matrix for Top "+sizeOfTop+" Words in "+perTopicOccurance.first()._1+" (Headline & Title):\n")
    df_2.show

    spark.stop()
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
