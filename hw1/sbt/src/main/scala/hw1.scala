import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.io.{File,PrintWriter}

object HW1 {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: HW1 <inputFile> <outputFile> <outputLog>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("HW1")
      .getOrCreate()

    import spark.implicits._

    // Load File
    val data = spark.sparkContext.textFile(args(0))


    // Remove Header
    val header = data.first
    val rows = data.filter(l => l != header)
    val hArray = Array("Date", "Time", "Active_Power", "Reactive_Power","Voltage","Intensity")

    //Flatten Date
    val flattenData = rows.
        flatMap{ dataString =>
            dataString.split(";").
                zipWithIndex.
                filter{
                    case (value,index) => index >= 2 && index <= 5 && value !="?"
                }.map{
                    case (value,index) => (hArray(index),value.toDouble)
                }
        }
    flattenData.persist()

    // Task 1 - Find Min, Max & Count
    val count = spark.sparkContext.broadcast(flattenData.map{case (k,v) => (k,1)}.reduceByKey((i, j) => i+j).collectAsMap())
    val max = spark.sparkContext.broadcast(flattenData.reduceByKey{(i, j) => if (i>j) i else j}.collectAsMap())
    val min = spark.sparkContext.broadcast(flattenData.reduceByKey{(i, j) => if (i<j) i else j}.collectAsMap())

    //Task 2 - Find Average
    val average = spark.sparkContext.
                    broadcast(flattenData.reduceByKey((i, j) => i+j).map{case (i, j) => (i,j/2049280)}.collectAsMap())
    val std = flattenData.
        map{case (k,v) => (k, scala.math.pow(v-average.value(k),2))}.
        reduceByKey((i,j) => i+j).
        map{case (k,v) => (k, math.sqrt(v/count.value(k)))}.collectAsMap()

    //Task 3 - Normalize Data
    val dataDF = rows.
                    map(_.split(";")).
                    map(att => (att(0), att(1),
                    if(att(2)=="?") Double.NaN else (att(2).toDouble-min.value(hArray(2)))/(max.value(hArray(2))-min.value(hArray(2))),
                    if(att(3)=="?") Double.NaN else (att(3).toDouble-min.value(hArray(3)))/(max.value(hArray(3))-min.value(hArray(3))),
                    if(att(4)=="?") Double.NaN else (att(4).toDouble-min.value(hArray(4)))/(max.value(hArray(4))-min.value(hArray(4))),
                    if(att(5)=="?") Double.NaN else (att(5).toDouble-min.value(hArray(5)))/(max.value(hArray(5))-min.value(hArray(5)))
                    )).
                    toDF(hArray(0),hArray(1),hArray(2),hArray(3),hArray(4),hArray(5))
    dataDF.write.csv(args(1))
    dataDF.createOrReplaceTempView("records")

    //Output Result
    val writer = new PrintWriter(args(2))
    writer.println("Spark Entity:       " + spark)
    writer.println("Spark version:      " + spark.version)
    writer.println("Spark master:       " + spark.sparkContext.master)
    writer.println("Running 'locally'?: " + spark.sparkContext.isLocal)
    writer.println("")

    def myprint(s: String): Unit = {
        writer.println("For "+s+":")
        writer.println("        Number of Meaningful Data - " + count.value(s))
        writer.println("        Maximum Value - " + max.value(s))
        writer.println("        Minimum Value - " + min.value(s))
        writer.println("        Mean - " + average.value(s))
        writer.println("        Standard Deviation - " + std(s) + "\n")
    }


    myprint(hArray(2))
    myprint(hArray(3))
    myprint(hArray(4))
    myprint(hArray(5))
    writer.println("Normalize Output Examples printed on Console View.")

    println("\n\nNormalized Output (Selected Examples)\n")
    spark.sql("SELECT * FROM records WHERE date = '28/4/2007'").show(25)

    spark.stop()
    writer.close()
  }
}
