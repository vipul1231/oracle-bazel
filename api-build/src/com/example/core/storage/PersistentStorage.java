package com.example.core.storage;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * Common interface for persistent object storage services (GCS, S3, Azure Blob storage) (Currently this interface is
 * specific for use by Trident)
 */
public interface PersistentStorage {

    /** Returns cloud storage root folder */
    String getRoot();

    /** Returns true if there are no files in persistent storage, otherwise false */
    boolean isEmpty();

    /**
     * Uploads files to persistent storage. Existing files will be overwritten.
     *
     * @param localFiles List of local file {@link Path}s located on local disk. Subfolders are not supported.
     */
    PersistentStorageResult<Path> upload(Set<Path> localFiles);

    /**
     * Uploads files to persistent storage and validate their integrity using MD5 checksum if provided. Existing files
     * in persistent storage will be overwritten.
     *
     * @param localFilesWithChecksum Map of file {@link Path}s located on local disk (note: subfolders are not
     *     supported) and their MD5 checksums in hex format (or null if not available).
     */
    PersistentStorageResult<Path> upload(Map<Path, String> localFilesWithChecksum);

    /**
     * Downloads all files from the persistent storage. Existing files in the local folder will be overwritten.
     *
     * @param targetLocalDir local directory where to save the downloaded files. {@code IllegalArgumentException} is
     *     thrown if local directory does not exist.
     */
    PersistentStorageResult<Path> download(Path targetLocalDir);

    /**
     * Downloads list of files from persistent storage. Existing files in the local folder will be overwritten.
     *
     * @param targetLocalDir Local directory where to save the downloaded files. {@code IllegalArgumentException} is
     *     thrown if local directory doesn't exist.
     * @param remoteFiles Set of path strings. Subfolders are not supported.
     * @param checksums MD5 checksums for the list of files to be downloaded. May be partial or empty if checksums are
     *     not available.
     */
    PersistentStorageResult<Path> download(Path targetLocalDir, Set<String> remoteFiles, Map<Path, String> checksums);

    /**
     * Deletes list of files in persistent storage.
     *
     * @param remoteFiles List of paths for the files to be deleted. Subfolders are not supported. Trying to delete
     *     non-existent files will not produce an error.
     */
    PersistentStorageResult<String> delete(Set<String> remoteFiles);

    /** Deletes all files (if any) in persistent storage. */
    PersistentStorageResult<String> purge();

    /** Returns list of file names. */
    Set<String> getFileNames();

    PersistentStorage get();
}