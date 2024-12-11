package org.apache.wayang.java.operators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

/**
 * This is execution operator implements the {@link ParquetSource}.
 * Inspiration from JavaTextFileSource.java and
 */
public class JavaParquetSource extends ParquetSource implements JavaExecutionOperator {

    public JavaParquetSource(String inputUrl) {
        super(inputUrl);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaParquetSource(ParquetSource that) {
        super(that);
    }

    // Consider streaming from URL if File is not found
    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
        ChannelInstance[] inputs,
        ChannelInstance[] outputs,
        JavaExecutor javaExecutor,
        OperatorContext operatorContext
    ) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        String urlStr = this.getInputUrl().trim();
        Path path = new Path(urlStr);

        try {
            FileSystem fs = FileSystems.getFileSystem(urlStr).get();
            InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                inputFile
            ).build();

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
                .map(record -> new Record(record));
            ((StreamChannel.Instance) outputs[0]).accept(recordStream);

        } catch (IOException e) {
            e.printStackTrace();
            throw new WayangException(String.format("Reading from URL: %s failed.", urlStr), e);
        }

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Source operator takes no input channels");
    }

    @Override
    // Copied from JavaTextFileSource
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
