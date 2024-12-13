/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.wayang.basic.operators.ParquetRecordSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is execution operator implements the {@link ParquetSource}.
 * Inspiration from JavaTextFileSource.java and
 */
public class JavaParquetRecordSource extends ParquetRecordSource implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaParquetSource.class);

    public JavaParquetRecordSource(String inputUrl) {
        super(inputUrl);
    }

    public JavaParquetRecordSource(ParquetRecordSource that) {
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
        InputFile inputFile;

        try {
            inputFile = HadoopInputFile.fromPath(path, new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
            throw new WayangException(String.format("InvalidURL", urlStr), e);
        }

        try (
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                inputFile
            ).build()
        ) {
            logger.info(">>> Ready to stream the data from URL: " + urlStr);
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
                .map(record -> mapRecord(record));
            ((StreamChannel.Instance) outputs[0]).accept(recordStream);
        } catch (IOException e) {
            e.printStackTrace();
            throw new WayangException(String.format("Reading from URL: %s failed.", urlStr), e);
        }

        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        outputs[0].getLineage().addPredecessor(mainLineageNode);
        return mainLineageNode.collectAndMark();
    }
    // GenericRecord -> Record
    private Record mapRecord(GenericRecord gRecord) {
        Object[] values = gRecord
            .getSchema()
            .getFields()
            .stream()
            .map(x -> gRecord.get(x.name()))
            .toArray();
        return new Record(values);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("Source operator takes no input channels");
    }

    @Override
    // Copied from JavaTextFileSource
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
