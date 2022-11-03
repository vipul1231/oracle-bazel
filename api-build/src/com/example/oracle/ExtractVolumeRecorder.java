package com.example.oracle;

import com.example.core2.Output;
import com.example.db.DbRowSize;

import java.util.Map;

public class ExtractVolumeRecorder {
    private final OracleMetricsHelper metricsHelper;
    private final Output<OracleState> output;
    private long extractVol = 0L;
    private final String label;

    public ExtractVolumeRecorder(OracleMetricsHelper metricsHelper, Output<OracleState> output, String label) {
        this.metricsHelper = metricsHelper;
        this.output = output;
        this.label = label;
    }

    public ExtractVolumeRecorder updateExtractVolume(Map<String, Object> values) {
        return updateExtractVolume(DbRowSize.mapValues(values));
    }

    /**
     * Updates the extract byte count by bytes and records the value with the metrics helper
     *
     * @param bytes
     */
    public ExtractVolumeRecorder updateExtractVolume(long bytes) {
        if (bytes > 0) {
            extractVol += bytes;
            //metricsHelper.recordExtractVolume(bytes);
        }
        return this;
    }

    /** Record the accumulated extract volume, and then reset the byte counter. */
    public void flush() {
        metricsHelper.logExtractDataVolume(label, extractVol, output.getLastFlushedTime());

        extractVol = 0;
    }
}
