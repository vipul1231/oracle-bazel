package com.example.ibf.metrics;

import com.example.core.TableRef;
import io.micrometer.core.instrument.LongTaskTimer;

public enum IbfTimerSampler {
    IBF_IBF_QUERY(
            "Ibf IBF query timer",
            null,
            null,
            IbfSubPhaseTags.IBF_ENCODE_IBF_QUERY_EXECUTION.value()),

    IBF_IBF_DOWNLOAD(
            "Ibf IBF download timer",
            null,
            null,
            IbfSubPhaseTags.IBF_IBF_DOWNLOAD_DURATION.value()),

    IBF_IBF_DECODE(
            "Ibf IBF decode timer",
            null,
            null,
            IbfSubPhaseTags.IBF_IBF_DECODE_DURATION.value()),

    IBF_IBF_UPLOAD(
            "Ibf IBF upload timer",
            null,
            null,
            IbfSubPhaseTags.IBF_IBF_UPLOAD_DURATION.value()),
    ;

    private final String description;
    private final String phase;
    private final String subPhase;
    private final String smallerSubPhase;
    private LongTaskTimer.Sample sample;

    IbfTimerSampler(String description, String phase, String subPhase, String smallerSubPhase) {
        this.description = description;
        this.phase = phase;
        this.subPhase = subPhase;
        this.smallerSubPhase = smallerSubPhase;
    }

    public void start(TableRef table) {
//        sample =
//                exampleConnectorMetrics.get()
//                        .longTaskTimer(
//                                MetricDefinition.SYNC_DURATION,
//                                TagName.SYNC_PHASE.toString(),
//                                phase,
//                                TagName.SUB_PHASE.value,
//                                subPhase,
//                                TagName.SMALLER_SUB_PHASE.value,
//                                smallerSubPhase,
//                                TagName.TABLE.toString(),
//                                table.toString())
//                        .start();
    }

    public void stop() {
        if (sample != null) sample.stop();
        sample = null;
    }
}
