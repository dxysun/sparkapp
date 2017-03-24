/**
  * Created by dongx on 2017/3/14.
  */
import org.apache.spark._
import org.apache.spark.streaming._

object WordCountStreaming {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WordCountStreaming").setMaster("local[*]")//设置为本地运行模式，2个线程，一个监听，另一个处理数据

    val ssc = new StreamingContext(sparkConf, Seconds(10))// 时间间隔为20秒
    println("文件流之前")
    val lines = ssc.textFileStream("file:///upload/data")  //这里采用本地文件，当然你也可以采用HDFS文件
    println("文件流之后")
    lines.saveAsTextFiles("file:///upload/datatest/writeback")
    println("保存之后")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    println("打印之后")
    ssc.start()
    println("流开始")
    ssc.awaitTermination()
  }
}