/**
Inpiration: {@link https://github.com/jerolba/parquet-for-java-posts/blob/master/src/main/java/com/jerolba/parquet/avro/FromParquetUsingAvroWithGenericRecord.java}
**/

package com.example;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

public class ParquetReaderExample {

    public static void main(String[] args) throws IOException {
        Path path = new Path("data/sf1_supplier.parquet");
        InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());

        try (
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                inputFile
            ).build()
        ) {
            GenericRecord record = null;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
        }
    }
}
