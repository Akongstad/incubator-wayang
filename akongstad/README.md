# Useful commands for Advanced Datasystems @ ITU | Project 3 | Adding a Parquet Source Operator in Apache Wayang

## Running the compiled project using Docker

```bash
docker compose up
```

```bash
./bin/wayang-submit org.apache.wayang.apps.wordcount.Main java file://$(pwd)/README.md
```

Compile only changed module

```bash
mvn clean install -DskipTests -pl wayang-benchmark -Drat.skip=true
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
mvn clean install -DskipTests -pl wayang-benchmark -Drat.skip=true
mvn clean package -pl :wayang-assembly -Pdistribution
cd wayang-assembly/target/
tar -xvf apache-wayang-assembly-0.7.1-incubating-dist.tar.gz
cd wayang-0.7.1
echo "export WAYANG_HOME=$(pwd)" >> ~/.bashrc
echo "export PATH=${PATH}:${WAYANG_HOME}/bin" >> ~/.bashrc
source ~/.bashrc
./bin/wayang-submit org.apache.wayang.apps.wordcount.WordCountParquet java file://$(pwd)/README.md
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
mvn clean install -DskipTests -Drat.skip=true -Dlicense.skipAddThirdParty=true -Dlicense.skipCheckLicense -Dlicense.skipDownloadLicenses
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
