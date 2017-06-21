import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.types._
import java.util.Properties
/**
  * Created by dongx on 2017/3/15.
  */
object Sqlstreaming {
  case class RawDataRecord(category: String, text: String)
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Streamimg").setMaster("local[*]")
  //  val sparkConf = new SparkConf().setAppName("Streaming").setMaster("spark://ambari:7077")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //    val conf = new SparkConf().setAppName("TestNaiveBayes").setMaster("local[*]")
    //   val sc = new SparkContext(conf)
    val sc = ssc.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
//    var tarinpath = "file:///upload/train/"
    var tarinpath = "hdfs://master:9000/upload/train/"
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

    val schema = StructType(List(StructField("predict", DoubleType, true),StructField("label", DoubleType, true)))
    val prop = new Properties()
    prop.put("user", "root") //表示用户名是hadoop
    prop.put("password", "toor") //表示密码是hadoop
    prop.put("driver","com.mysql.jdbc.Driver") //表示驱动程序是com.mysql.jdbc.Driver
/*    val studentRDD = sc.parallelize(Array("3 Rongcheng M 26","4 Guanhua M 27")).map(_.split(" "))
    //下面要设置模式信息
    val schema = StructType(List(StructField("predict", DoubleType, true),StructField("label", DoubleType, true)))
    //下面创建Row对象，每个Row对象都是rowRDD中的一行
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
    val studentDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //下面创建一个prop变量用来保存JDBC连接参数
    val prop = new Properties()
    prop.put("user", "root") //表示用户名是root
    prop.put("password", "root") //表示密码是hadoop
    prop.put("driver","com.mysql.jdbc.Driver") //表示驱动程序是com.mysql.jdbc.Driver
    //下面就可以连接数据库，采用append模式，表示追加记录到数据库spark的student表中
    studentDataFrame.write.mode("append").jdbc("jdbc:mysql://localhost:3306/spark", "spark.student", prop)
*/

    // 指定监控的目录
//    var testpath = "file:///upload/source"
    var testpath = "hdfs://master:9000/upload/source/"
    val lines = ssc.textFileStream(testpath).map {
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
        case Row(label: String, features: Vector) =>
          LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
      }

      //对测试数据集使用训练模型进行分类预测
      val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

      //统计分类准确率
      testpredictionAndLabel.foreach(println)
      val rowRDD1 = testpredictionAndLabel.map(x => Row(x._1.toDouble,x._2.toDouble))
      val resultFrame = sqlContext.createDataFrame(rowRDD1, schema)
      resultFrame.write.mode("append").jdbc("jdbc:mysql://master:3306/spark?useSSL=false", "spark.result", prop)
      //  println(testpredictionAndLabel.count())
      var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
      println("output5：")
      println(testaccuracy)
    }

    // 启动Streaming
    println("启动Streaming")
    ssc.start()
    ssc.awaitTermination()
  }

}
