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
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;

public class SSBCsv {

    /**
     * Get all the distinc countries of customers.
     */
    public static String customer_experiment(
        WayangContext wayangContext,
        String filePath,
        String experimentName,
        int numRepetition,
        String datasetSf,
        String dataset
    ) {
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
            .withJobName("SSBCsv")
            .withUdfJarOf(SSBCsv.class);
        try {
            long start = System.currentTimeMillis();
            /* Start building the Apache WayangPlan */
            Collection<String> countries = planBuilder
                /* Read the text file */
                .readTextFile(filePath)
                .withName("Load file")
                .map(x -> x.split("(?!\\B\"[^\"]*),(?![^\"]*\"\\B)")[4]) // match commas that are not in quotes.
                .distinct()
                .collect();

            System.out.printf("Found %d Countries:\n", countries.size());
            var end = System.currentTimeMillis();
            // Columns: id, experiment_name, operator, dataset_name, dataset_sf, elapsed_time, repetition_nr
            var resultRecordCsv = String.format(
                "id,%s,TextFileSource,%s,%s,rows,%d,%d",
                experimentName,
                dataset,
                datasetSf,
                end - start,
                numRepetition
            );
            System.out.println(countries);
            System.out.println(resultRecordCsv);
            return resultRecordCsv;
        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
        return null;
    }

    /**
     * Get total number of products sold.
     */
    public static String lineorder_experiment(
        WayangContext wayangContext,
        String filePath,
        String experimentName,
        int numRepetition,
        String datasetSf,
        String dataset
    ) {
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
            .withJobName("SSBCsv")
            .withUdfJarOf(SSBCsv.class);
        try {
            long start = System.currentTimeMillis();
            /* Start building the Apache WayangPlan */
            Collection<Integer> orderCount = planBuilder
                /* Read the text file */
                .readTextFile(filePath)
                .withName("Load file")
                .map(x -> x.split("(?!\\B\"[^\"]*),(?![^\"]*\"\\B)")[8]) // match commas that are not in quotes.
                .map(x -> Integer.parseInt(x))
                // Sum the orders
                // Map to same key
                .map(orders -> new Tuple2<>("Total", orders))
                // Sum the orders
                .reduceByKey(
                    Tuple2::getField0,
                    (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .map(t -> t.getField1())
                .collect();

            orderCount.forEach(System.out::println);
            var end = System.currentTimeMillis();
            // Columns: id, experiment_name, operator, dataset_name, dataset_sf, elapsed_time, repetition_nr
            var resultRecordCsv = String.format(
                "id,%s,TextFileSource,%s,%s,rows,%d,%d",
                experimentName,
                dataset,
                datasetSf,
                end - start,
                numRepetition
            );
            System.out.println(resultRecordCsv);
            return resultRecordCsv;
        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
        return null;
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length == 0) {
            System.err.print(
                "Usage: <platform1>[,<platform2>]* <input file URL> <experiment_name>"
            );
            System.exit(1);
        }
        String filePath = args[1];
        String experimentName = args[2];
        String[] split = filePath.split("/"); // Filepath example:file://$(pwd)/data/lineorder/sf1_lineorder.csv
        String fileName = split[split.length - 1];
        String sf = fileName.split("_")[0];
        String dataset = fileName.split("_")[1].split("\\.")[0];

        WayangContext wayangContext = new WayangContext();
        for (String platform : args[0].split(",")) {
            switch (platform) {
                case "java":
                    wayangContext.register(Java.basicPlugin());
                    break;
                default:
                    System.err.format("Unknown platform: \"%s\"\n", platform);
                    System.exit(3);
                    return;
            }
        }
        /* Get a plan builder */

        String[] results = new String[5];
        // Run experiment
        switch (experimentName) {
            case "customer_countries":
                for (int i = 0; i < 5; i++) {
                    results[i] = customer_experiment(
                        wayangContext,
                        filePath,
                        experimentName,
                        i,
                        sf,
                        dataset
                    );
                }
                break;
            case "lineorder_orders":
                for (int i = 0; i < 5; i++) {
                    results[i] = lineorder_experiment(
                        wayangContext,
                        filePath,
                        experimentName,
                        i,
                        sf,
                        dataset
                    );
                }
                break;
            default:
                System.err.format("Unknown experiment: \"%s\"\n", experimentName);
                System.exit(3);
                return;
        }
        // Print results to stdout
        System.out.println("Printing Results-----------------");
        for (String result : results) {
            System.out.println(result);
        }
    }
}
