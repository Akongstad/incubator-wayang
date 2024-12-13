/**
Inpiration: {@link https://github.com/jerolba/parquet-for-java-posts/blob/master/src/main/java/com/jerolba/parquet/avro/FromParquetUsingAvroWithGenericRecord.java}
**/

package com.example;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.wayang.basic.data.Record;

public class ParquetReaderExampleStream {

    public static void main(String[] args) throws IOException {
        String urlStr = "data/sf1_supplier.parquet";
        Path path = new Path(urlStr);
        InputFile inputFile;
        try {
            inputFile = HadoopInputFile.fromPath(path, new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to open input file.");
        }

        try (
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                inputFile
            )
                .withConf(new Configuration())
                .build()
        ) {
            // Generate a stream from the Parquet reader.
            Stream<Record> recordStream = Stream.generate(() -> {
                try {
                    return reader.read();
                } catch (IOException e) {
                    // error while reading. Act as if EOF.
                    e.printStackTrace();
                    return null;
                }
            })
                .takeWhile(record -> record != null)
                // GenericRecord -> Record
                .map(record -> mapRecord(record));

            recordStream.forEach(r -> System.out.println(r.getString(0)));
        }
    }

    // Print from stream:
    private static Record mapRecord(GenericRecord gRecord) {
        Object[] values = gRecord
            .getSchema()
            .getFields()
            .stream()
            .map(x -> gRecord.get(x.name()))
            .toArray();
        return new Record(values);
    }
}
