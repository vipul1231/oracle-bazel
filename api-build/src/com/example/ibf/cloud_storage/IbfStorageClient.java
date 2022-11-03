package com.example.ibf.cloud_storage;

import java.io.InputStream;

public interface IbfStorageClient {
    InputStream get(String objectId);

    void put(String objectId, byte[] encrypt);
}
