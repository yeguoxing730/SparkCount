import java.util

import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext, function}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by uc203808 on 12/25/2017.
  */
object SparkCount {
  def main(args:Array[String]){
    val inputFile =  "C:\\IDEA\\ideaWorkspaces\\scalaworkspace\\SparkCount\\annotation.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
      .set("spark.driver.memory", "5g")
      .set("spark.testing.memory", "471859200")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
    sc.stop()
  }
  //spark stand
  //c:\spark\bin>spark-submit --class SparkCount  C:\IDEA\ideaWorkspaces\scalaworkspace\SparkCount\out\artifacts\SparkCount_jar\SparkCount.jar

}
