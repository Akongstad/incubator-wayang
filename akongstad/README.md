# Useful commands for Advanced Datasystems @ ITU | Project 3 | Adding a Parquet Source Operator in Apache Wayang

## Build All
```bash
mvn clean install -DskipTests -Drat.skip=true -Dmaven.javadoc.skip=true -Djacoco.skip=true
```
## Running the compiled project using Docker

```bash
docker compose up
```

```bash
./bin/wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md
```

Compile only changed module
```bash
cd wayang-commons && mvn clean install -DskipTests -pl wayang-basic -Drat.skip=true -Dmaven.javadoc.skip=true -Djacoco.skip=true && cd ..
cd wayang-platforms && mvn clean install -DskipTests -pl wayang-java -Drat.skip=true -  Dmaven.javadoc.skip=true -Djacoco.skip=true && cd ..
cd wayang-api && mvn clean install -DskipTests -pl wayang-api-scala-java -Drat.skip=true -Dmaven.javadoc.skip=true -Djacoco.skip=true && cd ..
mvn clean install -DskipTests -pl wayang-benchmark -Drat.skip=true -Dmaven.javadoc.skip=true -Djacoco.skip=true

./bin/wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md
./bin/wayang-submit org.apache.wayang.apps.workloads.ParquetRead java file://$(pwd)/data/supplier/sf1_supplier.parquet
./bin/wayang-submit org.apache.wayang.apps.workloads.SSBParquet java file://$(pwd)/data/supplier/sf1_supplier.parquet

```

## Running the Standalone Java ParquetReaderExample

```bash
cd akongstad/standalone-parquet-reader
```

```bash
../.././mvnw compile exec:java -Dexec.mainClass="com.example.ParquetReaderExample"
```

## All at once

```bash
mvn clean package -pl :wayang-assembly -Pdistribution -Dmaven.javadoc.skip=true -Djacoco.skip=true
cd wayang-assembly/target/
tar -xvf apache-wayang-assembly-0.7.1-incubating-dist.tar.gz
cd wayang-0.7.1
echo "export WAYANG_HOME=$(pwd)" >> ~/.bashrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.bashrc
source ~/.bashrc
cd /var/www/html && ./bin/wayang-submit org.apache.wayang.apps.workloads.ParquetRead java file://$(pwd)/data/supplier/sf1_supplier.parquet


```

## Run the container the first time via Docker

```bash
docker compose up
```

Connect to the Wayang container

```bash
docker exec -it apache-wayang-app bash
```

Compile wayang and run the tpch benchmark

In the root dir of wayang (/var/www/html)

```bash
mvn clean install -DskipTests -Drat.skip=true -Dmaven.javadoc.skip=true -Djacoco.skip=true
```

Packaging the project to build the executable:

```bash
mvn clean package -pl :wayang-assembly -Pdistribution
```

Extract the build

```bash
cd wayang-assembly/target/
tar -xvf apache-wayang-assembly-0.7.1-incubating-dist.tar.gz
cd wayang-0.7.1
```

add to path

```bash
echo "export WAYANG_HOME=$(pwd)" >> ~/.bashrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.bashrc
source ~/.bashrc
```

Run code:

```bash
./bin/wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md
```
