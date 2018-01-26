
package main.scala

import org.apache.spark.{SparkConf, SparkContext}

object PracticaMaster {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Practica Master")
    val sc = new SparkContext(conf)

    //    val configfile = scala.io.Source.fromFile("config.file").
    //      getLines.
    //      toList.map(_.split("="))

    val myrdd = sc.textFile("/home/osboxes/spark/20170101_0.csv").map(_.split(","))
        .map(x=>(x(1), (x(0), x(2), x(3))))

    myrdd.take(5).foreach(println)

    //    System.out.println("Total words: " + counts.count());
    //    counts.saveAsTextFile("/tmp/shakespeareWordCount");
  }

}
