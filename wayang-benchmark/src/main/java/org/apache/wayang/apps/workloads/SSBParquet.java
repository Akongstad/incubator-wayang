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
import java.util.Set;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaParquetRecordSource;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;

public class SSBParquet {

    public static void main(String[] args) throws IOException, URISyntaxException {
        try {
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
                    .withJobName("SSBParquet")
                    .withUdfJarOf(SSBParquet.class);

            /* Time execution*/
            long start = System.currentTimeMillis();
            /* Start building the Apache WayangPlan */
            Collection<String> countries = planBuilder
                    /* Read the text file */
                    .readParquetRecordFile(new JavaParquetRecordSource(args[1]))
                    .map(r -> {
                        try {
                            return r.getString(4);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    })
                    .distinct()
                    .collect();


            var end = System.currentTimeMillis();
            // Columns: id, experiment_name, operator, dataset_name, dataset_sf, elapsed_time, repetition_nr
            var resultRecordCsv = String.format(
                    "id,read_csv,TextFileSource,dataset_name,dataset_sf,%d,repetition_nr",
                    end - start
            );
            System.out.printf("Found %d Countries:\n", countries.size());
            System.out.println(countries);
            System.out.println(resultRecordCsv);
        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }
}
