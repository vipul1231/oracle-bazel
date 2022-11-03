package com.example.oracle;

import java.util.Objects;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/30/2021<br/>
 * Time: 11:50 AM<br/>
 * To change this template use File | Settings | File Templates.
 */

class BlockRange {
    public long min;
    public long max;
    public TablespaceType type = null;

    // Jackson dependency
    BlockRange() {}

    BlockRange(long min, long max, TablespaceType type) {
        this.min = min;
        this.max = max;
        this.type = type;
    }

    public BlockRange withMax(long max) {
        return new BlockRange(min, max, type);
    }

    public BlockRange withMin(long min) {
        return new BlockRange(min, max, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlockRange otherRange = (BlockRange) o;
        return min == otherRange.min && max == otherRange.max && type == otherRange.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, type);
    }
}