SCALA_HOME = /usr
SPARK_HOME = /home/aditya/spark-2.2.0-bin-hadoop2.7
TEMP_DIR_PATH=/home/aditya/folder/
MY_CLASSPATH = $(SCALA_HOME)/lib/*:$(SPARK_HOME)/jars/*:out:.
SPARK_OPTIONS=--conf spark.driver.extraJavaOptions=-DTEMP_DIR_PATH="$(TEMP_DIR_PATH)" --master local[*] --driver-memory 4G

all: build

build: compile jar

compile:
	mkdir -p out
	$(SCALA_HOME)/bin/scalac -cp "$(MY_CLASSPATH)" -d out src/main/scala/neu/pdpmr/project/*.scala

jar:	
	cp -r src/main/resources/* out/neu/pdpmr/project/
	cp -r META-INF/MANIFEST.MF out
	cd out; jar cvmf MANIFEST.MF model-aditya_dhvani_apoorv.jar * ../lib
	mv out/model-aditya_dhvani_apoorv.jar .

run:
	$(SPARK_HOME)/bin/spark-submit $(SPARK_OPTIONS) model-aditya_dhvani_apoorv.jar


report:
	Rscript -e 'rmarkdown::render("report.Rmd")'
