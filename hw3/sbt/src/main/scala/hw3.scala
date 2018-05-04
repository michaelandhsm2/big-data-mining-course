import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

import java.io.{File,PrintWriter}
import scala.collection.mutable.ListBuffer

object Task0 {
  def main(args: Array[String]): Unit = {

    Common.checkArgs(args, 3, "HW3 <inputFolder> <outputFolder> <outputLog(.txt)>")

    val spark = SparkSession.builder.appName("HW3").getOrCreate()

    var data = Common.getFile(args(0))
    val kShingleValue = 3
    var numOfHashFunctions = spark.sparkContext.broadcast(100)
    var prime = 21601
    var rowsPerBand = spark.sparkContext.broadcast(5)
    var jaccardSimilarity = spark.sparkContext.broadcast(0.8)


    //Step 0 - Preprocess
    var regex = """(?s)<REUTERS.*?NEWID="(\d*)".*?>.*?<TEXT.*?>(.*?)<\/TEXT>.*?<\/REUTERS>""".r
    var body_regex = """(?s).*?<BODY.*?>(.*?)<\/BODY>.*?""".r
    var newsParse = spark.sparkContext.emptyRDD[(Int, String)]

    data.map{
        path  =>
        val singleNewsParse = spark.sparkContext.wholeTextFiles(path).flatMap{
            case (_, string) =>
            val list =  ListBuffer[Tuple2[Int, String]]()
            for(m <- regex.findAllIn(string).matchData){
                for( n <- body_regex.findAllIn(m.group(2)).matchData){
                    list += (Tuple2(m.group(1).toInt, n.group(1).replace("\n Reuter\n&#3;","")))
                }
            }
            list.toSeq
        }
        newsParse = newsParse.union(singleNewsParse)
    }

    def wordPreProcess(input: Any): Array[String] = {
            var matchRegex = """([$]?(?:[\w]+(?:[\w',]*[\w]+)+|[\w]))""".r
            var list =  ListBuffer[String]()
            for(m <- matchRegex.findAllIn(input.toString.toLowerCase).matchData;
              e <- m.subgroups)
              list+=e
            list.toArray
        }

    val parseSentenceRegex = """([$]?(?:[\w]+(?:[\w',]*[\w]+)+|[\w]))""".r


    //Step 1 - K-Shingling
    var flattenShingles = newsParse.flatMap{
        case (id, string)  =>
        wordPreProcess(string).sliding(kShingleValue).map{
            wordVector =>
            (wordVector.mkString(" "), id)
        }
    }

    var matrixIndex = spark.sparkContext.broadcast(newsParse.map{
        case(id, string) =>
        id.toInt
    }.sortBy{
        case (id) => id
    }.zipWithIndex.collectAsMap())

    var groupedShingles = flattenShingles.groupByKey()


    var newsCount = spark.sparkContext.broadcast(newsParse.count())
    var shinglesCount = spark.sparkContext.broadcast(groupedShingles.count())


    var shingleMatrix = groupedShingles.map{
        case (text, iterable) =>
        var row = Array.fill(newsCount.value.toInt){false}
        iterable.toVector.map{
            id =>
            row(matrixIndex.value(id).toInt) = true
        }
        row
    }

    val shingleString = shingleMatrix.map{
          array =>
          array.map{bool => if(bool) '1' else '0'}.mkString(",")
    }

    shingleString.saveAsTextFile(args(1) + "/shingleMatrix")


    // Step 2 - MinHash
    val rand = scala.util.Random
    var hashFunctionMatrix = Array.fill(numOfHashFunctions.value,2){rand.nextInt(2000) - 1000}

    def hashFunction(input: Int): Array[Int] = {
        hashFunctionMatrix.map{
            array =>
            (Math.abs(array(0).toInt * input + array(1).toInt)%prime)%newsCount.value.toInt
        }
    }

    val signatureMatrix = shingleMatrix.zipWithIndex.mapPartitions{
        itr =>

        itr.map{
            case(rowArray, index) =>
            var matrix = Array.fill(numOfHashFunctions.value,newsCount.value.toInt){99999}
            var hashResult = hashFunction(index.toInt).toSeq
            rowArray.zipWithIndex.map{case (x,i) =>
                if(x!=false){
                    for(n <- 0 until numOfHashFunctions.value){
                        matrix(n)(i) = hashResult(n)
                    }
                }
            }
            matrix
        }

    }.reduce{
        case (matrix1, matrix2) =>
        for(y <- 0 until matrix1.length;
          x <- 0 until matrix1(y).length){
            if(matrix1(y)(x) > matrix2(y)(x)){
                matrix1(y)(x) = matrix2(y)(x)
            }
        }
        matrix1
    }

    val signatureString = signatureMatrix.map{
          array =>
          array.mkString(",")
    }.mkString("\n")

    val sig_writer =  Common.outputWriter(args(1) + "/signatureMatrix")
    try {sig_writer.write(signatureString)} finally {sig_writer.close()}

    val hashFunctionString = hashFunctionMatrix.map{
          array =>
          array.mkString(",")
    }.mkString("\n")

    val hash_writer =  Common.outputWriter(args(1) + "/hashFunctionMatrix")
    try {hash_writer.write(hashFunctionString)} finally {hash_writer.close()}

    //Step 3 - Locality Sensitive Hashing
    var bandMax = Math.ceil(numOfHashFunctions.value/rowsPerBand.value)
    var bandIndexMap = spark.sparkContext.parallelize((0 until bandMax.toInt).toList)
    var signatureBroadcast = spark.sparkContext.broadcast(signatureMatrix)

    var LSH_matrix = bandIndexMap.map{
        n =>
        var matrix = Array.fill(newsCount.value.toInt,newsCount.value.toInt){false}
        for(y <- 0 until newsCount.value.toInt;
          x <- (y + 1) until newsCount.value.toInt){
            var matchCounter = 0;
            for(count <- 0 until rowsPerBand.value.toInt){
                if(signatureBroadcast.value(n * rowsPerBand.value.toInt + count)(y) == signatureBroadcast.value(n * rowsPerBand.value.toInt + count)(x)){
                    matchCounter += 1;
                }
            }
            if(matchCounter.toDouble/rowsPerBand.value >= jaccardSimilarity.value){
                matrix(y)(x) = true;
                matrix(x)(y) = true;
            }
        }
        matrix
    }.reduce{
        case (matrix1, matrix2) =>
        for(y <- 0 until matrix1.length;
          x <- 0 until matrix1(y).length){
            if(matrix2(y)(x)){
                matrix1(y)(x) = true
            }
        }
        matrix1
    }

    var LSH_pairs = LSH_matrix.map{
        array =>
        var list =  ListBuffer[Int]()
        array.zipWithIndex.map{
            case (v, i) =>
            if(v==true){
                list+=i
            }
        }
        list.toSeq
    }

    val LSH_String = LSH_pairs.zipWithIndex.map{
          case(array, i) =>
          (i+1) + ": " + array.mkString(",")
    }.mkString("\n")

    val lsh_writer =  Common.outputWriter(args(1) + "/LSH_Pairs")
    try {lsh_writer.write(LSH_String)} finally {lsh_writer.close()}

    //Output Result
    val writer =  Common.outputWriter(args(2))
    Common.printSpark(writer, spark)
    writer.println("\n\nNumber of News Articles: "+newsCount.value+"\n")
    writer.println("\n"+ kShingleValue+"-Shingle Count: "+shinglesCount.value+"\n")
    writer.println("\nNumber of Hash Functions: "+numOfHashFunctions.value+"\n")
    writer.println("\nRows per Band: "+rowsPerBand.value+"\n")
    writer.println("\nMin-Jaccard Similarity: "+jaccardSimilarity.value+"\n")


    Common.printSample(writer, newsParse.take(2).mkString("\n\n"), "Parsed Sample","")
    Common.printSample(writer, flattenShingles.take(20).mkString("\n"), "Parsed Shingle Sample","")
    Common.printSample(writer, matrixIndex.value.take(5).mkString("\n"), "Ziped With Index Sample","")
    Common.printSample(writer, groupedShingles.take(5).mkString("\n"), "Grouped Shingle Sample","")

    spark.stop()
    writer.close()

  }
}


object Task1 {
  def main(args: Array[String]): Unit = {

        Common.checkArgs(args, 3, "HW3 <inputFolder> <outputFolder> <outputLog(.txt)>")

        val spark = SparkSession.builder.appName("HW3").getOrCreate()

        var data = Common.getFile(args(0))
        val kShingleValue = 3
        var numOfHashFunctions = spark.sparkContext.broadcast(100)
        var prime = 21601
        var rowsPerBand = spark.sparkContext.broadcast(5)
        var jaccardSimilarity = spark.sparkContext.broadcast(0.8)


        //Step 0 - Preprocess
        var regex = """(?s)<REUTERS.*?NEWID="(\d*)".*?>.*?<TEXT.*?>(.*?)<\/TEXT>.*?<\/REUTERS>""".r
        var body_regex = """(?s).*?<BODY.*?>(.*?)<\/BODY>.*?""".r
        var newsParse = spark.sparkContext.emptyRDD[(Int, String)]

        data.map{
            path  =>
            val singleNewsParse = spark.sparkContext.wholeTextFiles(path).flatMap{
                case (_, string) =>
                val list =  ListBuffer[Tuple2[Int, String]]()
                for(m <- regex.findAllIn(string).matchData){
                    for( n <- body_regex.findAllIn(m.group(2)).matchData){
                        list += (Tuple2(m.group(1).toInt, n.group(1).replace("\n Reuter\n&#3;","")))
                    }
                }
                list.toSeq
            }
            newsParse = newsParse.union(singleNewsParse)
        }

        def wordPreProcess(input: Any): Array[String] = {
                var matchRegex = """([$]?(?:[\w]+(?:[\w',]*[\w]+)+|[\w]))""".r
                var list =  ListBuffer[String]()
                for(m <- matchRegex.findAllIn(input.toString.toLowerCase).matchData;
                  e <- m.subgroups)
                  list+=e
                list.toArray
            }

        val parseSentenceRegex = """([$]?(?:[\w]+(?:[\w',]*[\w]+)+|[\w]))""".r


        //Step 1 - K-Shingling
        var flattenShingles = newsParse.flatMap{
            case (id, string)  =>
            wordPreProcess(string).sliding(kShingleValue).map{
                wordVector =>
                (wordVector.mkString(" "), id)
            }
        }

        var matrixIndex = spark.sparkContext.broadcast(newsParse.map{
            case(id, string) =>
            id.toInt
        }.sortBy{
            case (id) => id
        }.zipWithIndex.collectAsMap())

        var groupedShingles = flattenShingles.groupByKey()


        var newsCount = spark.sparkContext.broadcast(newsParse.count())
        var shinglesCount = spark.sparkContext.broadcast(groupedShingles.count())


        var shingleMatrix = groupedShingles.map{
            case (text, iterable) =>
            var row = Array.fill(newsCount.value.toInt){false}
            iterable.toVector.map{
                id =>
                row(matrixIndex.value(id).toInt) = true
            }
            row
        }

        // Step 2 - MinHash
        val rand = scala.util.Random
        var hashFunctionMatrix = Array.fill(numOfHashFunctions.value,2){rand.nextInt(2000) - 1000}

        def hashFunction(input: Int): Array[Int] = {
            hashFunctionMatrix.map{
                array =>
                (Math.abs(array(0).toInt * input + array(1).toInt)%prime)%newsCount.value.toInt
            }
        }

        val signatureMatrix = shingleMatrix.zipWithIndex.mapPartitions{
            itr =>

            itr.map{
                case(rowArray, index) =>
                var matrix = Array.fill(numOfHashFunctions.value,newsCount.value.toInt){99999}
                var hashResult = hashFunction(index.toInt).toSeq
                rowArray.zipWithIndex.map{case (x,i) =>
                    if(x!=false){
                        for(n <- 0 until numOfHashFunctions.value){
                            matrix(n)(i) = hashResult(n)
                        }
                    }
                }
                matrix
            }

        }.reduce{
            case (matrix1, matrix2) =>
            for(y <- 0 until matrix1.length;
              x <- 0 until matrix1(y).length){
                if(matrix1(y)(x) > matrix2(y)(x)){
                    matrix1(y)(x) = matrix2(y)(x)
                }
            }
            matrix1
        }

        val signatureString = signatureMatrix.map{
              array =>
              array.mkString(",")
        }.mkString("\n")

        val sig_writer =  Common.outputWriter(args(1) + "/signatureMatrix")
        try {sig_writer.write(signatureString)} finally {sig_writer.close()}

        val hashFunctionString = hashFunctionMatrix.map{
              array =>
              array.mkString(",")
        }.mkString("\n")

        val hash_writer =  Common.outputWriter(args(1) + "/hashFunctionMatrix")
        try {hash_writer.write(hashFunctionString)} finally {hash_writer.close()}

        //Step 3 - Locality Sensitive Hashing
        var bandMax = Math.ceil(numOfHashFunctions.value/rowsPerBand.value)
        var bandIndexMap = spark.sparkContext.parallelize((0 until bandMax.toInt).toList)
        var signatureBroadcast = spark.sparkContext.broadcast(signatureMatrix)

        var LSH_matrix = bandIndexMap.map{
            n =>
            var matrix = Array.fill(newsCount.value.toInt,newsCount.value.toInt){false}
            for(y <- 0 until newsCount.value.toInt;
              x <- (y + 1) until newsCount.value.toInt){
                var matchCounter = 0;
                for(count <- 0 until rowsPerBand.value.toInt){
                    if(signatureBroadcast.value(n * rowsPerBand.value.toInt + count)(y) == signatureBroadcast.value(n * rowsPerBand.value.toInt + count)(x)){
                        matchCounter += 1;
                    }
                }
                if(matchCounter.toDouble/rowsPerBand.value >= jaccardSimilarity.value){
                    matrix(y)(x) = true;
                    matrix(x)(y) = true;
                }
            }
            matrix
        }.reduce{
            case (matrix1, matrix2) =>
            for(y <- 0 until matrix1.length;
              x <- 0 until matrix1(y).length){
                if(matrix2(y)(x)){
                    matrix1(y)(x) = true
                }
            }
            matrix1
        }

        var LSH_pairs = LSH_matrix.map{
            array =>
            var list =  ListBuffer[Int]()
            array.zipWithIndex.map{
                case (v, i) =>
                if(v==true){
                    list+=i
                }
            }
            list.toSeq
        }

        val LSH_String = LSH_pairs.zipWithIndex.map{
              case(array, i) =>
              (i+1) + ": " + array.mkString(",")
        }.mkString("\n")

        val lsh_writer =  Common.outputWriter(args(1) + "/LSH_Pairs")
        try {lsh_writer.write(LSH_String)} finally {lsh_writer.close()}

        //Output Result
        val writer =  Common.outputWriter(args(2))
        Common.printSpark(writer, spark)
        writer.println("\n\nNumber of News Articles: "+newsCount.value+"\n")
        writer.println("\n"+ kShingleValue+"-Shingle Count: "+shinglesCount.value+"\n")
        writer.println("\nNumber of Hash Functions: "+numOfHashFunctions.value+"\n")
        writer.println("\nRows per Band: "+rowsPerBand.value+"\n")
        writer.println("\nMin-Jaccard Similarity: "+jaccardSimilarity.value+"\n")


        Common.printSample(writer, newsParse.take(2).mkString("\n\n"), "Parsed Sample","")
        Common.printSample(writer, flattenShingles.take(20).mkString("\n"), "Parsed Shingle Sample","")
        Common.printSample(writer, matrixIndex.value.take(5).mkString("\n"), "Ziped With Index Sample","")
        Common.printSample(writer, groupedShingles.take(5).mkString("\n"), "Grouped Shingle Sample","")

        spark.stop()
        writer.close()
  }
}

object Log {
  def main(args: Array[String]): Unit = {

        Common.checkArgs(args, 3, "HW3 <inputFolder> <outputFolder> <outputLog(.txt)>")

        val spark = SparkSession.builder.appName("HW3").getOrCreate()

        var data = Common.getFile(args(0))
        val kShingleValue = 3
        var numOfHashFunctions = spark.sparkContext.broadcast(100)
        var prime = 21601
        var rowsPerBand = spark.sparkContext.broadcast(5)
        var jaccardSimilarity = spark.sparkContext.broadcast(0.8)


        //Step 0 - Preprocess
        var regex = """(?s)<REUTERS.*?NEWID="(\d*)".*?>.*?<TEXT.*?>(.*?)<\/TEXT>.*?<\/REUTERS>""".r
        var body_regex = """(?s).*?<BODY.*?>(.*?)<\/BODY>.*?""".r
        var newsParse = spark.sparkContext.emptyRDD[(Int, String)]

        data.map{
            path  =>
            val singleNewsParse = spark.sparkContext.wholeTextFiles(path).flatMap{
                case (_, string) =>
                val list =  ListBuffer[Tuple2[Int, String]]()
                for(m <- regex.findAllIn(string).matchData){
                    for( n <- body_regex.findAllIn(m.group(2)).matchData){
                        list += (Tuple2(m.group(1).toInt, n.group(1).replace("\n Reuter\n&#3;","")))
                    }
                }
                list.toSeq
            }
            newsParse = newsParse.union(singleNewsParse)
        }

        def wordPreProcess(input: Any): Array[String] = {
                var matchRegex = """([$]?(?:[\w]+(?:[\w',]*[\w]+)+|[\w]))""".r
                var list =  ListBuffer[String]()
                for(m <- matchRegex.findAllIn(input.toString.toLowerCase).matchData;
                  e <- m.subgroups)
                  list+=e
                list.toArray
            }

        val parseSentenceRegex = """([$]?(?:[\w]+(?:[\w',]*[\w]+)+|[\w]))""".r


        //Step 1 - K-Shingling
        var flattenShingles = newsParse.flatMap{
            case (id, string)  =>
            wordPreProcess(string).sliding(kShingleValue).map{
                wordVector =>
                (wordVector.mkString(" "), id)
            }
        }

        var matrixIndex = spark.sparkContext.broadcast(newsParse.map{
            case(id, string) =>
            id.toInt
        }.sortBy{
            case (id) => id
        }.zipWithIndex.collectAsMap())

        var groupedShingles = flattenShingles.groupByKey()


        var newsCount = spark.sparkContext.broadcast(newsParse.count())
        var shinglesCount = spark.sparkContext.broadcast(groupedShingles.count())


        var shingleMatrix = groupedShingles.map{
            case (text, iterable) =>
            var row = Array.fill(newsCount.value.toInt){false}
            iterable.toVector.map{
                id =>
                row(matrixIndex.value(id).toInt) = true
            }
            row
        }

        //Output Result
        val writer =  Common.outputWriter(args(2))
        Common.printSpark(writer, spark)
        writer.println("\n\nNumber of News Articles: "+newsCount.value+"\n")
        writer.println("\n"+ kShingleValue+"-Shingle Count: "+shinglesCount.value+"\n")
        writer.println("\nNumber of Hash Functions: "+numOfHashFunctions.value+"\n")
        writer.println("\nRows per Band: "+rowsPerBand.value+"\n")
        writer.println("\nMin-Jaccard Similarity: "+jaccardSimilarity.value+"\n")


        Common.printSample(writer, newsParse.take(2).mkString("\n\n"), "Parsed Sample","")
        Common.printSample(writer, flattenShingles.take(20).mkString("\n"), "Parsed Shingle Sample","")
        Common.printSample(writer, matrixIndex.value.take(5).mkString("\n"), "Ziped With Index Sample","")
        Common.printSample(writer, groupedShingles.take(5).mkString("\n"), "Grouped Shingle Sample","")

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
