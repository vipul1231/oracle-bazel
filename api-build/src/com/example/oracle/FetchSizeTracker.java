package com.example.oracle;

import java.sql.ResultSet;
import java.time.Duration;

public class FetchSizeTracker {

    private int startFetchSize;

    private int defaultFetchSize;

    private Duration duration;

    //@TODO what is this unknown field
    private int unknownVar;
    public FetchSizeTracker(int startFetchSize, int defaultFetchSize, Duration ofSeconds, int i) {

        this.startFetchSize = startFetchSize;
        this.defaultFetchSize = defaultFetchSize;
        this.duration = ofSeconds;
        this.unknownVar = i;

    }

    /**
     * @TODo: Need to write business logic
     * @param rows
     */
    public void update(ResultSet rows) {
    }

    /**
     * @TODO: Need to write business logic
     */
    public void resetLastUpdate() {
    }
}
