import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.io.File

object HW1 {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: HW1 <inputFile> <outputFile>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("HW1")
      .getOrCreate()

    import spark.implicits._

    def sparkPrint():Unit = {
      println("Spark Entity:       " + spark)
      println("Spark version:      " + spark.version)
      println("Spark master:       " + spark.sparkContext.master)
      println("Running 'locally'?: " + spark.sparkContext.isLocal)
      println("\n")
    }
    sparkPrint()

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

    // Task 1 - Find Min, Max & Count
    val count = flattenData.map{case (k,v) => (k,1)}.reduceByKey((i, j) => i+j).collectAsMap()
    val max = flattenData.reduceByKey{(i, j) => if (i>j) i else j}.collectAsMap()
    val min = flattenData.reduceByKey{(i, j) => if (i<j) i else j}.collectAsMap()

    //Task 2 - Find Average
    val average = flattenData.reduceByKey((i, j) => i+j).map{case (i, j) => (i,j/2049280)}.collectAsMap()
    val std = flattenData.
        map{case (k,v) => (k, scala.math.pow(v-average(k),2))}.
        reduceByKey((i,j) => i+j).
        map{case (k,v) => (k, math.sqrt(v/count(k)))}.collectAsMap()

    //Task 3 - Normalize Data
    val dataDF = rows.
                    map(_.split(";")).
                    map(att => (att(0), att(1),
                    if(att(2)=="?") Double.NaN else (att(2).toDouble-min(hArray(2)))/(max(hArray(2))-min(hArray(2))),
                    if(att(3)=="?") Double.NaN else (att(3).toDouble-min(hArray(3)))/(max(hArray(3))-min(hArray(3))),
                    if(att(4)=="?") Double.NaN else (att(4).toDouble-min(hArray(4)))/(max(hArray(4))-min(hArray(4))),
                    if(att(5)=="?") Double.NaN else (att(5).toDouble-min(hArray(5)))/(max(hArray(5))-min(hArray(5)))
                    )).
                    toDF(hArray(0),hArray(1),hArray(2),hArray(3),hArray(4),hArray(5))
    dataDF.write.csv(args(1))
    dataDF.createOrReplaceTempView("records")

    //Output Result
    def myprint(s: String): Unit = {
        println("For "+s+":")
        println("        Number of Meaningful Data - " + count(s))
        println("        Maximum Value - " + max(s))
        println("        Minimum Value - " + min(s))
        println("        Mean - " + average(s))
        println("        Standard Deviation - " + std(s) + "\n")
    }
    sparkPrint()

    myprint(hArray(2))
    myprint(hArray(3))
    myprint(hArray(4))
    myprint(hArray(5))
    println("\nNormalized Output (Selected Examples)\n")
    spark.sql("SELECT * FROM records WHERE date = '28/4/2007'").show(25)

    spark.stop()

  }
}
