package com.example.ibf.cloud_storage;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class MemoryStorageClient implements IbfStorageClient {
    private Map<String,byte[]> storage;

    public MemoryStorageClient() {
        storage = new HashMap<>();
    }

    @Override
    public InputStream get(String objectId) {
       return new ByteArrayInputStream(storage.get(objectId));
    }

    @Override
    public void put(String objectId, byte[] encrypt) {
        storage.put(objectId, encrypt);
    }
}
