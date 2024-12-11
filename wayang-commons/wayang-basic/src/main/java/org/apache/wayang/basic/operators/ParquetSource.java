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

package org.apache.wayang.basic.operators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

/**
 * This source reads a parquet file and outputs records as data units.
 *
 */
public class ParquetSource extends UnarySource<Record> {

    private final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Location of the parquet file
     */
    private final String inputUrl;

    public ParquetSource(String inputUrl) {
        super(DataSetType.createDefault(Record.class));
        this.inputUrl = inputUrl;
    }

    public String getInputUrl() {
        return this.inputUrl;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public ParquetSource(ParquetSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
    }
}
//TODO: Overwrite createCardinalityEstimator() for performance
