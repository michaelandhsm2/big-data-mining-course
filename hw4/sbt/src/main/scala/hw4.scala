import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import math.{sqrt, pow, abs}

import java.io.{File,PrintWriter}
import scala.collection.mutable.ListBuffer

object Task12 {
  def main(args: Array[String]): Unit = {

    Common.checkArgs(args, 3, "HW4 <inputFolder> <outputFolder> <outputLog(.txt)>")

    val spark = SparkSession.builder.appName("HW4").getOrCreate()

    var data = Common.getFile(args(0))

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

    var newsCount = spark.sparkContext.broadcast(newsParse.count())
    var matrixIndex = spark.sparkContext.broadcast(newsParse.map{
        case(id, string) =>
        id.toInt
    }.sortBy{
        case (id) => id
    }.zipWithIndex.collectAsMap())


    var flattenTerms = newsParse.flatMap{
        case (id, string)  =>
        wordPreProcess(string).map{
            word =>
            ((word, matrixIndex.value(id).toInt), 1)
        }
    }.reduceByKey{
        (x, y) => x + y
    }.map{
        case((word, id),count) =>
        (word,(id, count))
    }.groupByKey()

    var termDocumentMatrix = flattenTerms.map{
        case (word, iterable) =>
        var row = Array.fill(newsCount.value.toInt){0}
        iterable.toVector.map{
            case(id, count) =>
            row(id) = count
        }
        row
    }

    val termDocumentString = termDocumentMatrix.map{
          array =>
          array.mkString(",")
    }

    termDocumentString.saveAsTextFile(args(1)+"/termDocumentString")

    val rand = scala.util.Random
    val r_value = 3
    var otherMatrix = spark.sparkContext.parallelize(Array.fill(newsCount.value.toInt,r_value){rand.nextInt(2000) - 1000})

    import org.apache.spark.rdd.RDD
    def matrixMultiplication(matrix1: RDD[Array[Int]], matrix2: RDD[Array[Int]]): RDD[Array[Int]] = {
        var otherMatrix = spark.sparkContext.broadcast(matrix2.zipWithIndex.map{x => (x._2, x._1)}.collectAsMap())
        matrix1.map{
            array =>
            var newRow = Array.fill(otherMatrix.value(0).length){0}
            newRow.zipWithIndex.map{
                case(value, columnid) =>
                var newValue = value
                array.zipWithIndex.foreach{
                    case(arrayValue, rowid) =>
                    newValue += arrayValue * otherMatrix.value(rowid)(columnid)
                }
                newValue
            }
        }
    }

    var multipliedMatrix = matrixMultiplication(termDocumentMatrix, otherMatrix)

    var mulString = multipliedMatrix.map{
          array =>
          array.mkString(",")
    }

    mulString.saveAsTextFile(args(1)+"/multipliedMatrix")



    //Output Result
    val writer =  Common.outputWriter(args(2))

    Common.printSpark(writer, spark)
    writer.println("\nCount: "+ newsCount.value+"\n")
    Common.printSample(writer, newsParse.take(2).mkString("\n\n"), "Parsed Sample", "")
    Common.printSample(writer, matrixIndex.value.take(5).mkString("\n"), "Ziped With Index Sample", "")
    Common.printSample(writer, flattenTerms.take(20).mkString("\n"), "Parsed Shingle Sample", "")
    Common.printSample(writer, multipliedMatrix.take(2).map{ array => array.mkString(", ")}.mkString("\n"),
     "New Matrix Sample", "")

    spark.stop()
    writer.close()

  }
}


object Task3 {
  def main(args: Array[String]): Unit = {

        Common.checkArgs(args, 3, "HW4 <inputFolder> <outputFolder> <numOfNews>")

        val spark = SparkSession.builder.appName("HW4").getOrCreate()

        var data = Common.getFile(args(0))

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
        newsParse = spark.sparkContext.parallelize(newsParse.take(args(2).toInt))

        def wordPreProcess(input: Any): Array[String] = {
                var matchRegex = """([$]?(?:[\w]+(?:[\w',]*[\w]+)+|[\w]))""".r
                var list =  ListBuffer[String]()
                for(m <- matchRegex.findAllIn(input.toString.toLowerCase).matchData;
                  e <- m.subgroups)
                  list+=e
                list.toArray
            }

        var newsCount = spark.sparkContext.broadcast(newsParse.count())
        var matrixIndex = spark.sparkContext.broadcast(newsParse.map{
            case(id, string) =>
            id.toInt
        }.sortBy{
            case (id) => id
        }.zipWithIndex.collectAsMap())

        var flattenTerms = newsParse.flatMap{
            case (id, string)  =>
            wordPreProcess(string).map{
                word =>
                ((word, matrixIndex.value(id).toInt), 1)
            }
        }.reduceByKey{
            (x, y) => x + y
        }.map{
            case((word, id),count) =>
            (word,(id, count))
        }.groupByKey()

        var termDocumentMatrix = flattenTerms.map{
            case (word, iterable) =>
            var row = Array.fill(newsCount.value.toInt){0}
            iterable.toVector.map{
                case(id, count) =>
                row(id) = count
            }
            row
        }

        def matrixDoubleMultiplication(matrix1: RDD[Array[Double]], matrix2: RDD[Array[Double]]): RDD[Array[Double]] = {
            var otherMatrix = spark.sparkContext.broadcast(matrix2.zipWithIndex.map{x => (x._2, x._1)}.collectAsMap())
            matrix1.map{
                array =>
                var newRow = Array.fill(otherMatrix.value(0).length){0.0}
                newRow.zipWithIndex.map{
                    case(value, columnid) =>
                    var newValue = value
                    array.zipWithIndex.foreach{
                        case(arrayValue, rowid) =>
                        newValue += arrayValue * otherMatrix.value(rowid)(columnid)
                    }
                    newValue
                }
            }
        }

        def matrixSubtract(matrix1: RDD[Array[Double]], matrix2: RDD[Array[Double]]): RDD[Array[Double]] = {
            var otherMatrix = spark.sparkContext.broadcast(matrix2.zipWithIndex.map{x => (x._2, x._1)}.collectAsMap())
            matrix1.zipWithIndex.map{
                case (array, rowid) =>
                array.zipWithIndex.map{
                    case(value, columnid) =>
                    value - otherMatrix.value(rowid)(columnid)
                }
            }
        }

        def matrixTranspose(matrix: RDD[Array[Double]]): RDD[Array[Double]] = {
            var transpose = matrix.collect.toSeq.transpose.map{seq => seq.toArray}
            spark.sparkContext.parallelize(transpose)
        }


        def matrixAbs(matrix: RDD[Array[Double]]): Double = {
            var sum = matrix.map{
                array =>
                array.map{
                    value =>
                    pow(value, 2)
                }
            }.reduce((a,b) => Array.fill(1){a(0) + b(0)})
            sqrt(sum(0))
        }

        var count = 0
        var doubleTermMatrix = termDocumentMatrix.map{array => array.map{x => x.toDouble}}
        var transposeMatrix = matrixTranspose(doubleTermMatrix)
        var symmetricMatrix = matrixDoubleMultiplication(doubleTermMatrix, transposeMatrix)

        var eigenValue = 0.0

        while(count < 5 && !eigenValue.isNaN){
            var xMatrix = spark.sparkContext.parallelize(Array.fill(symmetricMatrix.count().toInt,1){1.0})
            var prevDiv = 0.0
            var div = 10.0
            var countNum = 0

            while(abs(div-prevDiv) >= 0.00000000000001){
                countNum += 1
                prevDiv = div
                var mul = matrixDoubleMultiplication(symmetricMatrix, xMatrix)
                div = matrixAbs(xMatrix)

                xMatrix = mul.map{
                    array=>
                    array.map{ value => value/div }
                }
            }

            var transposeX = matrixTranspose(xMatrix)
            var partial = matrixDoubleMultiplication(transposeX, symmetricMatrix)
            var eigen = matrixDoubleMultiplication(partial, xMatrix)
            eigenValue = eigen.collect()(0)(0)

            println(countNum +": "+eigenValue)
            xMatrix.map{array => array.mkString(", ")}.saveAsTextFile(args(1)+"/SVD/"+eigenValue)

            var partial2 = matrixDoubleMultiplication(xMatrix, transposeX)
            var partial3 = partial2.map{
                array=>
                array.map{ value => value * eigenValue }
            }

            symmetricMatrix = matrixSubtract(symmetricMatrix, partial3)

            count += 1
        }
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
