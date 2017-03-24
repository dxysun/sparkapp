/**
  * Created by dongx on 2017/3/14.
  */
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming {
  case class RawDataRecord(category: String, text: String)
  def main(args: Array[String]) {
  //  val sparkConf = new SparkConf().setAppName("Streaming").setMaster("yarn-cluster")
 //   val sparkConf = new SparkConf().setAppName("Streaming").setMaster("spark://master:7077")
//    val sparkConf = new SparkConf().setAppName("Streaming").setMaster("yarn-client")
    val sparkConf = new SparkConf().setAppName("Streamimg").setMaster("local[*]")
//    val sparkConf = new SparkConf().setAppName("Streaming").setMaster("spark://ambari:7077")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
//    val conf = new SparkConf().setAppName("TestNaiveBayes").setMaster("spark://ambari:7077 ")
//   val sc = new SparkContext(conf)
    val sc = ssc.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var tarinpath = "file:///upload/train/"
 //   var tarinpath = "hdfs://master:9000/upload/train/"
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
    println("output4：")

    //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")
    println("train finished")




    // 指定监控的目录
  //  val lines = ssc.textFileStream("hdfs://master:9000/upload/source")
    val lines = ssc.textFileStream("file:///upload/source")
    val data = lines.map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }
    data.foreachRDD{(rdd, time) =>
      var testDF = rdd.toDF()
      var testwordsData = tokenizer.transform(testDF)
      var testfeaturizedData = hashingTF.transform(testwordsData)
      var testrescaledData = idfModel.transform(testfeaturizedData)
      var testDataRdd = testrescaledData.select($"category",$"features").map {
        case Row(label: String, features: Vector) =>
          LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
      }
      var testDataRdd2 = testrescaledData.select($"category",$"text",$"features").map {
        case Row(category:String,text: String,features: Vector) =>
          (category,text, Vectors.dense(features.toArray))
      }

      //对测试数据集使用训练模型进行分类预测
      val testpredictionAndLabel1 = testDataRdd2.map(p => (p._1,model.predict(p._3)))
      //对测试数据集使用训练模型进行分类预测
   //   val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

      //统计分类准确率
      testpredictionAndLabel1.foreach(println)
      //  println(testpredictionAndLabel.count())
    //  var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
      println("output5：")
   //   println(testaccuracy)
    }

    // 启动Streaming
    println("启动Streaming")
    ssc.start()
    ssc.awaitTermination()
  }
}
