package com.example.core;

import com.example.core2.Output;

import java.time.Duration;

/** This class is responsible for periodically checkpointing the state. */
public class PeriodicCheckpointer<State> {
    private final Output<State> output;
    private final State state;
    private final long period;
    private long nextCheckpointTime;

    public PeriodicCheckpointer(Output<State> output, State state, Duration period) {
        this.output = output;
        this.state = state;
        this.period = period.toMillis();
        this.nextCheckpointTime = 0;
    }

    private synchronized boolean shouldCheckpoint() {
        long currentTime = System.currentTimeMillis();
        if (currentTime >= nextCheckpointTime) {
            nextCheckpointTime = currentTime + period;
            return true;
        }

        return false;
    }

    /**
     * maybeCheckpoint checkpoints the state if more time than [period] has elapsed since the last call to checkpoint
     * method
     */
    public void maybeCheckpoint() {
        if (shouldCheckpoint()) {
            output.checkpoint(state);
        }
    }
}
