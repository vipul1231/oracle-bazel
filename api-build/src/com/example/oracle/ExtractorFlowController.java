package com.example.oracle;

import com.example.crypto.OracleRecordQueue;

import java.time.Duration;

public class ExtractorFlowController {

    private Duration accumulatedWait;
    public void resetTotalWait() {
    }

    /**
     * @TODO: Need to write business logic
     * @return
     */
    public Duration getTotalWait() {
        return null;
    }

    /**
     * @TODO: Need to write business logic
     * @param recordQueue
     */
    public void flowControl(OracleRecordQueue recordQueue) {
    }

    public Duration getAccumulatedWait() {
        return accumulatedWait;
    }

    public void setAccumulatedWait(Duration accumulatedWait) {
        this.accumulatedWait = accumulatedWait;
    }
}
