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

package org.apache.wayang.apps.workloads;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaParquetRecordSource;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;

public class ParquetRecordRead {

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length == 0) {
            System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
            System.exit(1);
        }

        WayangContext wayangContext = new WayangContext();
        for (String platform : args[0].split(",")) {
            switch (platform) {
                case "java":
                    wayangContext.register(Java.basicPlugin());
                    break;
                case "spark":
                    wayangContext.register(Spark.basicPlugin());
                    break;
                default:
                    System.err.format("Unknown platform: \"%s\"\n", platform);
                    System.exit(3);
                    return;
            }
        }

        /* Get a plan builder */
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
            .withJobName("Read")
            .withUdfJarOf(ParquetRecordRead.class);

        String filePath = args[1];
        String experimentName = "read";
        String[] split = filePath.split("/"); // Filepath example:file://$(pwd)/data/lineorder/sf1_lineorder.csv
        String fileName = split[split.length - 1];
        String sf = fileName.split("_")[0];
        String dataset = "customer";
        String operator = "ParquetRecordSource";
        String[] results = new String[5];

        for (int i = 0; i < 5; i++) {
            try {
                // Start timing
                long start = System.currentTimeMillis();

                // Build and execute the WayangPlan
                Collection<Record> entries = planBuilder
                    .readParquetRecordFile(new JavaParquetRecordSource(args[1]))
                    .collect();

                // End timing
                long end = System.currentTimeMillis();

                // Format result as a CSV row
                var resultRecordCsv = String.format(
                    "id,%s,%s,%s,%s,%d,%d",
                    experimentName,
                    operator,
                    dataset,
                    sf,
                    end - start,
                    i
                );

                // Store result in the array
                results[i] = resultRecordCsv;

                // Print details for this run
                System.out.printf("Repetition %d: Found %d entries.\n", i, entries.size());
                entries.stream().limit(10).forEach(x -> System.out.println(x));
            } catch (Exception e) {
                System.err.printf("Repetition %d failed.\n", i);
                e.printStackTrace();
            }
        }

        // Print the results
        Arrays.stream(results).forEach(System.out::println);
    }
}