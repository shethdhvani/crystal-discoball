package neu.pdpmr.project

import java.io._
import java.util.zip.GZIPInputStream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.io.Source


object Model {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    var tempDir = args(0)
    if (tempDir.charAt(tempDir.length - 1) != '/') {
      tempDir += "/"
    }

    val featuresSchema = StructType(
      Array(
        StructField("artist_name", StringType, true),
        StructField("title", StringType, true),
        StructField("td_confidence", StringType, true),
        StructField("artist_hotness", StringType, true),
        StructField("song_hotness", StringType, true),
        StructField("td_meanPrice", StringType, true)

      )
    )

    val averageSchema = StructType(
      Array(

        StructField("mean_artist_hotness", StringType, true),
        StructField("mean_song_hotness", StringType, true),
        StructField("mean_td_meanPrice", StringType, true),
        StructField("mean_td_confidence", StringType, true)

      )
    )


    //Get Features from the compressed csv file stored in jar
    val featuresDF = getFeaturesFromResource("features.csv.gz", spark, featuresSchema, "features")
    featuresDF.createOrReplaceTempView("features")

    val averageDF = getFeaturesFromResource("averages.csv.gz", spark, averageSchema, "average")
    val averageFeatureRow = averageDF.rdd.map(row => row.toString().replace("[", "").replace("]", "").split(",")).take(1)(0)

    var inputBuffer = new ListBuffer[String]()

    val input = Source.fromInputStream(System.in)
    var ok = true
    while (ok) {
      val ln = readLine()
      ok = ln != null
      if (ok) {

        if (ln.split(";").length != 2) {
          ok = false
        }
        else{
          inputBuffer += ln
        }
      }
    }


    var featureBuffer = new ListBuffer[Array[String]]()

    for (i <- 0 to inputBuffer.length - 1) {
      //process input
      featureBuffer.append(getFeaturesForGivenInput(inputBuffer(i), spark, averageFeatureRow))
    }

    val featureData = spark.sparkContext.parallelize(featureBuffer)
    val testingData = featureData.map(row => {
      Vectors.dense(row(0).toDouble, row(1).toDouble, row(2).toDouble, row(3).toDouble)
    }
    )

    moveModel(tempDir)
    val model = RandomForestModel.load(spark.sparkContext, tempDir + "model")
    val predicions = model.predict(testingData)
    predicions.collect().foreach(x=>println(x.toInt))

  }

  def getFeaturesForGivenInput(input: String, spark: SparkSession, avgArray: Array[String]): Array[String] = {
    //split input string by ;
    val arr = input.split(";")
    val artist = arr(0)
    val title = arr(1)


    var selectColumns = "td_confidence,artist_hotness,song_hotness,td_meanPrice "
    var condtions = "artist_name =" + "'" + cleanString(artist) + "'" + " and title =" + "'" + cleanString(title) + "'"
    val q = spark.sql("select " + selectColumns + "from features where " + condtions)
    //three conditions
    var rowCount = q.count()
    if (rowCount == 1) {
      return q.rdd.map(row => row.toString().replace("[", "").replace("]", "").split(",")).take(1)(0)
    }
    else if (rowCount != 0) {
      getAverageFeaturesFromDF(q)
    }
    else {
      //two conditions
      val arr1 = getFeaturesForGivenArtistName(artist, spark)
      if (arr1(0) == "noArray") {
        val arr2 = getFeaturesForGivenSongTitle(title, spark)
        if (arr2(0) == "noArray") {
          return avgArray
        }
        else {
          return arr2
        }
      }
      else {
        return arr1
      }

    }
  }

  // method to filter features table based on song title
  def getFeaturesForGivenSongTitle(title: String, sparkSession: SparkSession): Array[String] = {
    var selectColumns = "td_confidence,artist_hotness,song_hotness,td_meanPrice "
    var condtions = "title =" + "'" + cleanString(title) + "'"
    val q = sparkSession.sql("select " + selectColumns + "from features where " + condtions)
    //three conditions
    var rowCount = q.count()
    if (rowCount == 1) {
      return q.rdd.map(row => row.toString().replace("[", "").replace("]", "").split(",")).take(1)(0)
    }
    else if (rowCount != 0) {
      return getAverageFeaturesFromDF(q)
    }
    else {
      return Array("noArray")
    }
  }


  // method to filter features table based on artist title
  def getFeaturesForGivenArtistName(artistName: String, sparkSession: SparkSession): Array[String] = {
    var selectColumns = "td_confidence,artist_hotness,song_hotness,td_meanPrice "
    var condtions = "artist_name =" + "'" + cleanString(artistName) + "'"
    val q = sparkSession.sql("select " + selectColumns + "from features where " + condtions)
    //three conditions
    var rowCount = q.count()
    if (rowCount == 1) {
      return q.rdd.map(row => row.toString().replace("[", "").replace("]", "").split(",")).take(1)(0)
    }
    else if (rowCount != 0) {
      return getAverageFeaturesFromDF(q)
    }
    else {
      return Array("noArray")
    }
  }

  // method to compute the average of the 4 features we are using
  def getAverageFeaturesFromDF(df: DataFrame): Array[String] = {
    val avgArtistHotness = df.select(avg("artist_hotness")).rdd.map(row => row.toString().replace("[","").replace("]","")).take(1)(0)
    val avgSongHotness = df.select(avg("song_hotness")).rdd.map(row => row.toString().replace("[","").replace("]","")).take(1)(0)
    val avgMeanPrice = df.select(avg("td_meanPrice")).rdd.map(row => row.toString().replace("[","").replace("]","")).take(1)(0)
    return Array("3", avgArtistHotness, avgSongHotness, avgMeanPrice)
  }

  // helper method to remove all non alphanumeric characters
  def cleanString(s: String): String = {
    s.replaceAll("[^A-Za-z0-9]", "").toLowerCase()
  }

  // method to read the in the resource folder
  def getFeaturesFromResource(s: String, spark: SparkSession, customSchema: StructType, typ: String): DataFrame = {
    var featuresStream = getClass.getResourceAsStream(s)
    val lines = Source.fromInputStream(new GZIPInputStream(featuresStream)).getLines()
    val data = spark.sparkContext.parallelize(lines.toList)


    val first = data.first()

    if (typ == "features") {
      val rowRDD = data.filter(row => row != first).map(row => getRowFromArray(row.split(",")))
      spark.createDataFrame(rowRDD, customSchema)
    }
    else {
      val rowRDD = data.filter(row => row != first).map(row => getAverageRowFromArray(row.split(",")))
      spark.createDataFrame(rowRDD, customSchema)
    }

  }

  // helper method to to transform feature array into Row
  def getRowFromArray(arr: Array[String]): Row = {
    Row(arr(0), arr(1), arr(9), arr(3), arr(2), arr(8))
  }

  // helper method to transform averages array into Row
  def getAverageRowFromArray(arr: Array[String]): Row = {
    Row(arr(0), arr(1), arr(2), arr(3))
  }



  // these 3 methods are hacks to load the model into a temp file from the resource folder.
  private def copyStream(is: InputStream, f: File): Boolean = {
    try
      return copyStream(is, new FileOutputStream(f))
    catch {
      case e: FileNotFoundException =>
        e.printStackTrace()
    }
    false
  }


  private def copyStream(is: InputStream, os: FileOutputStream): Boolean = {
    try {
      val buf = new Array[Byte](1024)
      var len = 0
      while (len != -1) {
        len = is.read(buf)
        if (len != -1) {
          os.write(buf, 0, len)
        }
      }
      is.close
      os.close
      return true
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    false
  }

  def moveModel(tempDir: String): Unit = {
    import scala.io.Source
    val modelName = "model"
    val modelTempDir = new File(tempDir + modelName)
    modelTempDir.mkdir()
    val modelTempDataDir = new File(tempDir + modelName + "/data")
    modelTempDataDir.mkdir()
    val modelTempMetadataDir = new File(tempDir + modelName + "/metadata")
    modelTempMetadataDir.mkdir()
    val bufferedSource = getClass().getResourceAsStream("files.csv")
    val src = Source.fromInputStream(bufferedSource).getLines().mkString("\n")
    val dataFiles = src.split("\n")(0)
    val metadataFiles = src.split("\n")(1)
    for (file <- dataFiles.split(",")) {
      val f = new File(modelTempDataDir + "/" + file)
      copyStream(getClass.getResourceAsStream(file), f)
    }
    for (file <- metadataFiles.split(",")) {
      val f = new File(modelTempMetadataDir + "/" + file)
      copyStream(getClass.getResourceAsStream(file), f)
    }
  }


}