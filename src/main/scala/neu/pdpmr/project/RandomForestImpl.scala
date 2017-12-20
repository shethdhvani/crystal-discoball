// Authors: Aditya, Apoorv, Dhvani
// This is the program for training the random forest model. We load the training and testing data from two different
// files. This is to avoid "producer effect" i.e. a song from an artist is not there in both data sets.
// We also read a feature.txt file which has the number of columns (comma separated) to use as features from
// all_features_train.csv/all_features_test.csv. We give confidence as categorical column and train the model.
// We then calculate the errors and write it to a file. We run our model on training data as well, so as to check
// whether we have overfitted the model or not. The less difference between RMSE of training and test data, the better.
package neu.pdpmr.project
import java.io.{BufferedOutputStream, PrintWriter}

import breeze.numerics.abs
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object RandomForestImpl {

  private val log: Logger = Logger.getLogger(RandomForestImpl.getClass)

  // create RDD from a CSV file
  def getCSVLines(sc: SparkContext, fileName: String, sep:String, headerPresent: Boolean): RDD[Array[String]] = {
    val fileRDD = sc.textFile(fileName)
    if (headerPresent) {
      val header = fileRDD.first()
      fileRDD
        .filter(row => row != header)
        .map(row => row.split(sep))
    } else {
      fileRDD
        .map(row => row.split(sep))
    }
  }

  // convert time to milliseconds
  def toMs(start: Long, end: Long): Long = {
    (end - start) / (1000L * 1000L)
  }

  // write to file
  def write(it: Iterable[Iterable[String]], headerIt: Iterable[String], path: Path, sc: SparkContext,
    headerPresent: Boolean): Unit = {
    val fs = FileSystem.get(path.toUri, sc.hadoopConfiguration)
    val pw: PrintWriter = new PrintWriter(new BufferedOutputStream(fs.create(path)))
    if (headerPresent) {
      pw.println(headerIt.mkString(";"))
    }
    it.foreach(x => pw.println(x.mkString(";")))
    pw.close
  }

  // initially we had 15 features. For analyzing, we were removing certain features. Thus reading from this file which
  // has columns as numbers which represent which features to give to the model
  def getColumnsToInclude(arrString:Array[String], arrDouble:List[Double]): Array[Double] ={
      val columnList: ListBuffer[Double] = new ListBuffer[Double]
      for (i <- 0 to arrString.length-1) {
        if (arrDouble.contains(i)) {
          columnList.append(arrString(i).toDouble)
        }
      }
    columnList.toArray
  }

  def main(args: Array[String]) {
    var startOverall = System.nanoTime()
    var start = -1L
    var trainingDataFile = args(0)
    var testingDataFile = args(1)
    var featureFile = args(2)
    var outputDir = args(3)
    var bm = new ListBuffer[Array[String]]()
    val bmHeader = Array("impl", "step", "timeInMs")
    var writeMetrics = new ListBuffer[Array[String]]()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("RandomForestRegression")
    if (conf.get("master", "") == "") {
      log.info("Setting master internally")
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)

    val featureInputString = getCSVLines(sc,featureFile,",", false)
    var featuresWanted = featureInputString.map(row => {row.map(x => x.toDouble)}).take(1)(0).toList
    var featureSet = featuresWanted.toSet

    start = System.nanoTime()
    // get training data
    val trainingData = getCSVLines(sc, trainingDataFile, ",", true)
    val trainingDataPoints = trainingData.map(row =>
      new LabeledPoint(
        row(16).toDouble,
        Vectors.dense(getColumnsToInclude(row,featuresWanted))
      )
    )

    // get testing data
    val testingData = getCSVLines(sc, testingDataFile, ",", true)
    val testingDataPoints = testingData.map(row =>
      new LabeledPoint(
        row(16).toDouble,
        Vectors.dense(getColumnsToInclude(row,featuresWanted))
      )
    )

    bm.append(Array(sc.master, "read data", toMs(start, System.nanoTime()).toString()))

    start = System.nanoTime()
    // Empty categoricalFeaturesInfo indicates all features are continuous. If we have a 0 in the feature.txt file means
    // we are considering confidence as one of our features to the model and thus take confidence as a categorical
    // feature. Confidence has values ranging from 0 to 5. Thus using it as a categorical feature. The random
    // forest model internally maps it six different columns with 0 in all columns except for which the value
    // was specified
    var categoricalFeaturesInfo = Map[Int, Int]()
    if (featureSet.contains(0.0)) {
      categoricalFeaturesInfo = Map[Int, Int](
        (0, 6)
      )
    }

    val numTrees = args(4).toInt
    val featureSubsetStrategy = "auto" // Let the algorithm choose
    val impurity = "variance"
    val maxDepth = args(5).toInt
    val maxBins = 100

    // Train a RandomForest model
    val model = RandomForest.trainRegressor(trainingDataPoints, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    bm.append(Array(sc.master, "training data", toMs(start, System.nanoTime()).toString()))

    model.save(sc, args(6))

    for (i <- 0 to 1) {
      var name = ""
      if (i == 0) {
        name = "testing data"
      } else {
        name = "training data"
      }
      start = System.nanoTime()
      // Evaluate model on test instances and compute test error
      val labelsAndPredictions = testingDataPoints.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      bm.append(Array(sc.master, name, toMs(start, System.nanoTime()).toString()))

      val getDiff = labelsAndPredictions.map(row => (row._1.toInt, row._2.toInt, abs(row._1.toInt - row._2.toInt)))
      getDiff.saveAsTextFile(outputDir + "//" + name + "/actualPredictedValues")

      start = System.nanoTime()
      val testMSE = labelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
      println("Evaluation metrics for " + name)
      println("Test Mean Squared Error = " + testMSE)
      writeMetrics.append(Array("Test Mean Squared Error " + name, testMSE.toString()))

      val predictionAndLabels = labelsAndPredictions.map { case (s, c) => (c, s) }
      // Instantiate metrics object
      val metrics = new RegressionMetrics(predictionAndLabels)
      // Squared error
      println(s"MSE = ${metrics.meanSquaredError}")
      writeMetrics.append(Array("MSE " + name, metrics.meanSquaredError.toString()))
      println(s"RMSE = ${metrics.rootMeanSquaredError}")
      writeMetrics.append(Array("RMSE " + name, metrics.rootMeanSquaredError.toString()))
      // R-squared
      println(s"R-squared = ${metrics.r2}")
      writeMetrics.append(Array("R-squared " + name, metrics.r2.toString()))
      // Mean absolute error
      println(s"MAE = ${metrics.meanAbsoluteError}")
      writeMetrics.append(Array("MAE " + name, metrics.meanAbsoluteError.toString()))
      // Explained variance
      println(s"Explained variance = ${metrics.explainedVariance}")
      writeMetrics.append(Array("Explained variance " + name, metrics.explainedVariance.toString()))
      bm.append(Array(sc.master, "getting evaluation metrics " + name, toMs(start, System.nanoTime()).toString()))
    }

    bm.append(Array(sc.master, "total time", toMs(startOverall, System.nanoTime()).toString()))
    write(bm.map(_.toList), bmHeader.toList, new Path(outputDir, "bm.csv"), sc, true)
    write(writeMetrics.map(_.toList), None, new Path(outputDir, "metrics.csv"), sc, false)
  }
}
