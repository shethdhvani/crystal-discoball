package neu.pdpmr.project



import java.io.{BufferedWriter, File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.control.Exception.allCatch
// Author : Aditya ,Dhvani,Apoorv

object Feature {

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    createSongInfoTable(spark,"/home/aditya/songs/all/song_info.csv")
    createUniqueSongTitleCombinationFromTrainingData(spark,"/home/aditya/downloads_file/downloads.csv")
    createPlayCountTable(spark,"/home/aditya/downloads_file/play_count.csv")
    createSimilarArtistTable(spark,"/home/aditya/songs/all/similar_artists.csv")
    createTopCommonGenereTable(spark,"/home/aditya/songs/all/artist_terms.csv")
    queries(spark)
    aggregation(spark)



  }

  def aggregation(spark:SparkSession):Unit={
    val df = spark.sql("select * from features")
    val avgArtistHotness = df.select(avg("artist_hotness")).rdd.map(row => row.toString()).take(1)(0)
    val avgSongHotness = df.select(avg("song_hotness")).rdd.map(row=>row.toString()).take(1)(0)
    val avgMeanPrice = df.select(avg("td_meanPrice")).rdd.map(row=>row.toString()).take(1)(0)
    var sb:StringBuilder = new StringBuilder()

    sb.append("mean_confidence")
    sb.append(",")
    sb.append("mean_artist_hotness")
    sb.append(",")
    sb.append("mean_song_hotness")
    sb.append(",")
    sb.append("mean_mean_price")
    sb.append("\n")
    sb.append("3")
    sb.append(",")
    sb.append(avgArtistHotness)
    sb.append(",")
    sb.append(avgSongHotness)
    sb.append(",")
    sb.append(avgMeanPrice)

    writeOutputToCsv(sb.toString(),"averages.csv")


    println(avgArtistHotness)
    println(avgSongHotness)
    println(avgMeanPrice)
  }

  def writeOutputToCsv(result:String,fileName:String): Unit ={

      val file = new File(fileName)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(result)
      bw.close()

  }

  def queries(spark:SparkSession): Unit ={

    val q1_select_columns = "artist_id,artist_name,title,song_hotness,artist_hotness,tempo,play_count,song_id"
    val q1_condtion_columns = "song_id = play_song_id and track_id = play_track_id"

    val q2_select_columns = ",similar_artist_count "

    val q4_select_columns = q1_select_columns + q2_select_columns + ",top_1000_count "
    val q4_condition_columns = "where artist_id = artistId3"

    val q3_select_columns = "artist_name,title,td_confidence,artist_hotness,song_hotness,td_meanPrice "
    val q3_condition_columns = "title = td_title and artist_name = td_artist"

    val q1 = spark.sql("select "+q1_select_columns +" from song_info left join play_count_table on " + q1_condtion_columns)
    q1.na.fill(0,Array("play_count")).createOrReplaceTempView("intermediate_feature_1")

    val q2 = spark.sql("select " + q1_select_columns + q2_select_columns + "from intermediate_feature_1 left join similar_artists on intermediate_feature_1.artist_id = similar_artists.artistId2")
    q2.na.fill(0,Array("similar_artist_count")).createOrReplaceTempView("intermediate_feature_2")

    val q4 = spark.sql("select " + q4_select_columns + "from intermediate_feature_2 ,artist_genre_common " + q4_condition_columns)
    q4.createOrReplaceTempView("intermediate_feature_3")

    val q3 = spark.sql("select " + q3_select_columns + " from intermediate_feature_3 left join training_data on " + q3_condition_columns)
    val features = q3.na.fill("0",Array("td_confidence","td_meanPrice"))
    features.createOrReplaceTempView("features")
    features.printSchema()
    features.show()
    features.rdd.saveAsTextFile("features_scattered")
    val r = spark.sparkContext.textFile("features_scattered")

    r.map(row => row.replace("[","").replace("]","")).coalesce(1).saveAsTextFile("features")


  }

  def createTopCommonGenereTable(spark:SparkSession,fileName:String):Unit={
    val artist_terms_df = getCSVIntoDF(spark,fileName,";")
    artist_terms_df.createOrReplaceTempView("artist_terms")
    val query = "select artist_terms.artist_term,artist_hotness from song_info,artist_terms where artist_terms.artist_id = song_info.artist_id"
    val q = spark.sql(query)
    q.createOrReplaceTempView("term_hotness")

    val query2 = "select artist_term,avg(artist_hotness) as artist_hotness from term_hotness group by artist_term order by artist_hotness desc limit 1000"
    val q2 = spark.sql(query2)
    q2.createOrReplaceTempView("top_1000_artist_terms")

    val top_1000_terms = spark.sql("select artist_term from top_1000_artist_terms").rdd.map(row=>row.toString().replace("[","").replace("]","")).take(1000).toSet


    val customSchema = StructType(
      Array(
        StructField("artistId3",StringType,true),
        StructField("top_1000_count",IntegerType,true))
    )

    val artist_top_20_genereRDD = artist_terms_df.
      rdd
      .map(row=>row.toString().replace("[","").replace("]","").split(","))
      .map(row=>(row(0),row(1)))
      .groupByKey()
      .map({case (x,y) => Row(x,intersection_query(y.toSet,top_1000_terms))})


    val df = spark.createDataFrame(artist_top_20_genereRDD,customSchema)
    df.createOrReplaceTempView("artist_genre_common")

  }
  def intersection_query(set:Set[String],top_1000_terms:Set[String]):Integer={

    set.intersect(top_1000_terms).size
  }



  def createSimilarArtistTable(spark:SparkSession,fileName:String):DataFrame={


    val similar_artists1 = getCSVIntoDF(spark,fileName,";")
    similar_artists1.createOrReplaceTempView("similar_artists")
    val q = spark.sql("select artist_id  as artistId2 ,count(similar_artist) as similar_artist_count from similar_artists group by artist_id")
    q.createOrReplaceTempView("similar_artists")
    q
  }

  def createPlayCountTable(spark:SparkSession,fileName:String): Unit ={
    val taste_profile_df = getCSVIntoDF(spark,fileName,",")
    taste_profile_df.printSchema()
    taste_profile_df.createOrReplaceTempView("play_count_table")
  }

  def createUniqueSongTitleCombinationFromTrainingData(spark:SparkSession, fileName:String): Unit ={
    val training_data = getCSVIntoRDD(spark.sparkContext,fileName,";")



    val temp2 = training_data.filter(row=> row.length == 5 && isDoubleNumber(row(2)))
    println(temp2.count)
    val trainingDataRDD = temp2.map(row=> Row(cleanString(row(0)),cleanString(row(1)),row(2),row(3),translateConfidence(row(4))))

    val customSchema = StructType(
      Array(
        StructField("td_artist",StringType,true),
        StructField("td_title",StringType,true),
        StructField("td_meanPrice",StringType,true),
        StructField("td_downloads",StringType,true),
        StructField("td_confidence",StringType,true))
    )

    val trainingDataF = spark.sqlContext.createDataFrame(trainingDataRDD,customSchema)
    trainingDataF.createOrReplaceTempView("training_data")

  }

  def cleanString(s:String) :String ={

    s.replaceAll("[^A-Za-z0-9]","").toLowerCase()
  }

  def getCSVIntoDF(spark:SparkSession,fileName:String,delimiter:String,header:String ="true"): DataFrame ={
    val df1 = spark.read
      .format("csv")
      .option("header", header) //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", delimiter)
      .load(fileName)

    df1
  }

  def translateConfidence(c:String):String={
    if(c.equals("terrible")){
      return "0"
    }
    if(c.equals("poor")){
      return "1"
    }
    if(c.equals("average")){
      return "2"
    }
    if(c.equals("good")){
      return "3"
    }
    if(c.equals("very good")){
      return "4"
    }
    if(c.equals("excellent")){
      return "5"
    }
    else{
      return "-1"
    }
  }

  def getCSVIntoRDD(sc: SparkContext, fileName: String,sep:String): RDD[Array[String]] = {
    val fileRDD = sc.textFile(fileName)
    val header = fileRDD.first()
    fileRDD
      .filter(row => row != header)
      .map(row => row.split(sep))
  }
  def createSongInfoTable(spark:SparkSession,fileName:String): Unit = {

    val songInfoDF = getCSVIntoDF(spark,fileName,";")
    songInfoDF.createOrReplaceTempView("song_info")
    songInfoDF.printSchema()
    val q = spark.sql("select artist_name,title,song_hotttnesss as song_hotness,artist_hotttnesss as artist_hotness,tempo,artist_id,song_id,track_id from song_info")
    q.printSchema()
    val cleanedRDD = q
                      .rdd
                      .map(row => cleanSongInfoRow(row.toString().replace("[","").replace("]","").split(",")))
                      .map(row => Row(row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7)))
    val songInfoSchema = StructType(
      Array(
        StructField("artist_name",StringType,true),
        StructField("title",StringType,true),
        StructField("song_hotness",StringType,true),
        StructField("artist_hotness",StringType,true),
        StructField("tempo",StringType,true),
        StructField("artist_id",StringType,true),
        StructField("song_id",StringType,true),
        StructField("track_id",StringType,true)

      ))

    val q2 = spark.createDataFrame(cleanedRDD,songInfoSchema)
    q2.createOrReplaceTempView("song_info")
    q2.printSchema()



  }
  def changeNaValueToZero(s:String):String={
    if(!isDoubleNumber(s)){
      return "0"
    }
    else{
      return s
    }
  }

  def cleanSongInfoRow(arr:Array[String]): Array[String] ={
    Array(cleanString(arr(0))
      ,cleanString(arr(1))
      ,changeNaValueToZero(arr(2))
      ,changeNaValueToZero(arr(3))
      ,changeNaValueToZero(arr(4)),arr(5),arr(6),arr(7))
  }


  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

}
