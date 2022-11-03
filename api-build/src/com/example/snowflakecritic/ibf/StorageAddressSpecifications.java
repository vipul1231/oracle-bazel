package com.example.snowflakecritic.ibf;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.*;

/**
 * The DTO of the {@link StorageEnvironmentVariable#CORE_STORAGE_WRITE} environment variable. Each non-null field in
 * this class can be used for accessing a single Example bucket in its respective storage service (AWS S3, GCS, Azure
 * Blob Storage).
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class StorageAddressSpecifications {
    private final LocalDiskStorageAddressSpecification local;

    public StorageAddressSpecifications(
             @JsonProperty("local") LocalDiskStorageAddressSpecification local) {
        this.local = local;
    }


    public Optional<LocalDiskStorageAddressSpecification> localDiskSpecification() {
        return Optional.ofNullable(local);
    }

    @Override
    public String toString() {
        return "StorageAddressSpecifications{"
                + ", local="
                + local
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StorageAddressSpecifications that = (StorageAddressSpecifications) o;
        return Objects.equals(local, that.local);
    }

    @Override
    public int hashCode() {
        return Objects.hash(local);
    }
}
