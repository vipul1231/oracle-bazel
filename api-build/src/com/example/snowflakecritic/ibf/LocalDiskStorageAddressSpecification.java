package com.example.snowflakecritic.ibf;

import java.nio.file.Path;
import java.util.Objects;

// marker class
public final class LocalDiskStorageAddressSpecification {
    private Path storageDir = null;

    /** We need this constructor for deserialization from prod DB, should not be used in other cases */
    public LocalDiskStorageAddressSpecification() {}

    public LocalDiskStorageAddressSpecification(Path storageDir) {
        this.storageDir = storageDir;
    }

    public Path getStorageDir() {
        return Objects.requireNonNull(storageDir, "Path to local disk storage was not defined");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o instanceof LocalDiskStorageAddressSpecification;
    }

    @Override
    public int hashCode() {
        return 42;
    }
}
