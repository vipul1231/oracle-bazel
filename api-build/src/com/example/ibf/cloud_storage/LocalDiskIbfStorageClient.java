package com.example.ibf.cloud_storage;

import com.example.snowflakecritic.ibf.LocalDiskStorageAddressSpecification;

import java.io.*;
import java.nio.file.Paths;

public class LocalDiskIbfStorageClient implements IbfStorageClient {
    private final LocalDiskStorageAddressSpecification specification;

    public LocalDiskIbfStorageClient(LocalDiskStorageAddressSpecification specification) {
        this.specification = specification;
    }

    @Override
    public InputStream get(String objectId) {
        try {
            return new FileInputStream(getFileForObjectId(objectId));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void put(String objectId, byte[] encrypt) {
        try {
            FileOutputStream outputStream = new FileOutputStream(getFileForObjectId(objectId), false);
            outputStream.write(encrypt);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private File getFileForObjectId(String objectId) {
        File file = new File(Paths.get(specification.getStorageDir().toString(), objectId).toString());
        return file;
    }
}
