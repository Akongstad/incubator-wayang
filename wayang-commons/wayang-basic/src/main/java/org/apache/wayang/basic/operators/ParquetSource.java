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
    //TODO: Overwrite createCardinalityEstimator() for performance
