package com.example.sql_server;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class ChangeEntry {
    public static final short VERSION = 1;

    final Operation op;
    final Instant opTime;
    final Long changeVersion;
    final BigInteger lsn;
    final Map<String, Object> values;

    ChangeEntry(Operation op, Instant opTime, Long changeVersion, BigInteger lsn, Map<String, Object> values) {
        this.op = op;
        this.opTime = opTime;
        this.changeVersion = changeVersion;
        this.lsn = lsn;
        this.values = Collections.unmodifiableMap(values);
    }

    public static ChangeEntry v1(
            Operation op, Instant opTime, Long changeVersion, BigInteger lsn, Map<String, Object> values) {
        if (changeVersion == null && lsn == null)
            throw new IllegalArgumentException("Change version and LSN cannot be null for a change entry");
        if (changeVersion != null && lsn != null)
            throw new IllegalArgumentException("Change version and LSN cannot both be non-null for a change entry");

        return new ChangeEntry(op, opTime, changeVersion, lsn, values);
    }

    @Override
    public String toString() {
        return "ChangeEntry{"
                + "op="
                + op
                + ", opTime="
                + opTime
                + ", changeVersion="
                + changeVersion
                + ", lsn="
                + lsn
                + ", values="
                + values
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChangeEntry that = (ChangeEntry) o;
        return op == that.op
                && Objects.equals(opTime, that.opTime)
                && Objects.equals(changeVersion, that.changeVersion)
                && Objects.equals(lsn, that.lsn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, opTime, changeVersion, lsn, values);
    }
}
