/**
  * Created by dongx on 2017/3/15.
  */
import java.util.Properties

import Sqlstreaming.RawDataRecord
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}
object Sqltest {
  case class RawDataRecord(category: String, text: String)
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Streamimg").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //    val conf = new SparkConf().setAppName("TestNaiveBayes").setMaster("local[*]")
    //   val sc = new SparkContext(conf)
    val sc = ssc.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var tarinpath = "file:///upload/train/"
    var trainRDD = sc.textFile(tarinpath).map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }

    var trainingDF = trainRDD.toDF()

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)
    println("output1：")
    wordsData.select($"category",$"text",$"words").take(1)

    //计算每个词在文档中的词频
    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    println("output2：")



    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    println("output3：")


    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    var trainDataRdd1 = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))

    }
    println("output4：")

    //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")
    println("train finished")
    val schema = StructType(List(StructField("filename", StringType, true),StructField("content", StringType, true),StructField("filetype", DoubleType, true)))

    val prop = new Properties()
    prop.put("user", "hadoop") //表示用户名是hadoop
    prop.put("password", "hadoop") //表示密码是hadoop
    prop.put("driver","com.mysql.jdbc.Driver") //表示驱动程序是com.mysql.jdbc.Driver

    // 指定监控的目录
    val lines = ssc.textFileStream("file:///upload/source").map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }

    lines.foreachRDD{(rdd, time) =>
      var testDF = rdd.toDF()
      var testwordsData = tokenizer.transform(testDF)
      var testfeaturizedData = hashingTF.transform(testwordsData)
      var testrescaledData = idfModel.transform(testfeaturizedData)
      var testDataRdd = testrescaledData.select($"category",$"features").map {
        case Row(label: String,features: Vector) =>
          LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
      }
      var testDataRdd2 = testrescaledData.select($"category",$"text",$"features").map {
          case Row(category:String,text: String,features: Vector) =>
            (category,text, Vectors.dense(features.toArray))
        }

      //对测试数据集使用训练模型进行分类预测
      val testpredictionAndLabel = testDataRdd2.map(p => (p._1,p._2,model.predict(p._3)))

      //统计分类准确率
      testpredictionAndLabel.foreach(println)
      val rowRDD1 = testpredictionAndLabel.map(x => Row(x._1.toString,x._2.toString,x._3.toDouble))
      val resultFrame = sqlContext.createDataFrame(rowRDD1, schema)
      resultFrame.write.mode("append").jdbc("jdbc:mysql://localhost:3306/hadoop?useSSL=false", "hadoop.spark_message", prop)
      //  println(testpredictionAndLabel.count())
   //   var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
      println("output5：")
  //    println(testaccuracy)
    }

    // 启动Streaming
    println("启动Streaming")
    ssc.start()
    ssc.awaitTermination()
  }
}
