import LearnNaiveBayes.RawDataRecord
import Testsvm.RawDataRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

/**
  * Created by dongx on 2017/3/14.
  */
object Learnsvm {


  case class RawDataRecord(category: String, text: String)

  def main(args : Array[String]) {

    //   val conf = new SparkConf().setMaster("yarn-client")
    //   val conf = new SparkConf().setAppName("TestNaiveBayes").setMaster("spark://master:7077")
    val conf = new SparkConf().setAppName("TestNaiveBayes").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var tarinpath = "file://" + args(0)
    var trainRDD = sc.textFile(tarinpath).map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }
    var testpath = "file://" + args(1)
    var testRDD = sc.textFile(testpath).map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }
    //70%作为训练数据，30%作为测试数据
  //  val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    var trainingDF = trainRDD.toDF()
    var testDF = testRDD.toDF()

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)
    println("output1：")
    wordsData.select($"category",$"text",$"words").take(1)

    //计算每个词在文档中的词频
    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    println("output2：")
    featurizedData.select($"category", $"words", $"rawFeatures").take(1)


    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    println("output3：")
    rescaledData.select($"category", $"features").take(1)

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    println("output4：")


    //训练模型
    //   val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")
    val numIterations = 10
    val model = SVMWithSGD.train(trainDataRdd, numIterations)
    //测试数据集，做同样的特征表示及格式转换
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
   // println(testpredictionAndLabel.count())
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("output5：")
    println(testaccuracy)

  }

}