SPARK_HOME = /home/aditya/spark-2.2.0-bin-hadoop2.7
CLASSPATH=`echo $(SPARK_HOME)/jars/*.jar | tr ' ' :`
SONG_INFO = /home/aditya/songs/all/song_info.csv
ARTIST_TERMS = /home/aditya/songs/all/artist_terms.csv
SIMILAR_ARTISTS = /home/aditya/songs/all/similar_artists.csv
OUTPUT_DIR_WITH_CACHE = output2
OUTPUT_DIR_WITHOUT_CACHE = output1
ITERATIONS = 10
all: build run

build:
	scalac -cp $(CLASSPATH) scala/Feature.scala -d Feature.jar

run:
	$(SPARK_HOME)/bin/spark-submit --class Feature \
        --master local[*] --deploy-mode client --executor-memory 4g \
        --name Feature --conf "spark.app.id=Feature" \
        Feature.jar input/all_features4.csv outputtrain input/features.txt

