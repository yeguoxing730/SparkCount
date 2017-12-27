import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by uc203808 on 12/27/2017.
  */
object ClusterWordCount {
  def  main(args:Array[String]){
    if (args.length == 0) {
      System.err.println("Usage: ClusterWorldCount args is null")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://10.35.47.60:7077")
        .setJars(List("C:\\IDEA\\ideaWorkspaces\\scalaworkspace\\SparkCount\\out\\artifacts\\SparkCount_jar\\SparkCount.jar"))
      .set("spark.driver.memory", "5g")
      .set("spark.testing.memory", "471859200")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val rdd = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    rdd.saveAsTextFile(path = args(1))
    sc.stop()
  }
  //sh spark-submit --master spark://apollo.hadoop.com:7077 --class ClusterWordCount /home/hadoop/SparkCount.jar hdfs://apollo.hadoop.com:9000/hadoop/input/wordcount.txt hdfs://apollo.hadoop.com:9000/hadoop/out_put/out.txt
  //hadoop fs -mkdir /hadoop/out_put
  //hadoop fs -rm /hadoop/out_put/out.txt
  //hadoop fs -cat /hadoop/out_put/out.txt/*
}
