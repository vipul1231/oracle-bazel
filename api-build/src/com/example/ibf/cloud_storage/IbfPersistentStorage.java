package com.example.ibf.cloud_storage;

import com.example.crypto.Encrypt;
import com.example.ibf.exception.DataNotFoundException;
import com.example.logger.ExampleLogger;
import com.example.snowflakecritic.ibf.LocalDiskStorageAddressSpecification;
import com.example.snowflakecritic.ibf.StorageAddressSpecifications;
import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.UUID;

public class IbfPersistentStorage {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    public static final String STORAGE_ENV_VAR_NAME = "IBF_STORAGE";
    private static final String ENCRYPTION_CONTEXT = "ibfPersistentStorage";
    private static final String ROOT = "ibf";

    private final IbfStorageClient storageClient;
    private final SecretKey secretKey;

    public IbfPersistentStorage(IbfStorageClient storageClient, SecretKey secretKey) {
        this.storageClient = storageClient;
        this.secretKey = secretKey;
    }

    /**
     * Update an object, identified by objectId, in storage. Its contents are encrypted before uploading.
     *
     * @param objectId unique identifier of the object
     * @param body     serialized object contents
     * @throws IOException
     */
    public void put(String objectId, ByteBuf body) throws IOException {
        storageClient.put(objectId, encrypt(body));
    }

    /**
     * Retrieve an object, identified by objectId, from storage. Its contents are decrypted after downloading.
     *
     * @param objectId unique identifier of the object
     * @throws IOException
     */
    public ByteBuf get(String objectId) throws IOException {
        InputStream in = storageClient.get(objectId);
        if (in == null) throw new DataNotFoundException("Cannot find data with objectId: " + objectId);
        return decrypt(in);
    }

    public static String generateRandomObjectId() {
        return UUID.randomUUID().toString();
    }

    public static Builder newBuilder(SecretKey secretKey) {
        return new Builder(secretKey);
    }

    public static class Builder {
        private StorageAddressSpecifications storageAddress;
        private IbfStorageClient storageClient;
        private SecretKey secretKey;

        public Builder(SecretKey secretKey) {
            this.secretKey = secretKey;
        }

        public Builder withLocalDiskStorage(Path storageDir) {
            this.storageAddress =
                    new StorageAddressSpecifications(new LocalDiskStorageAddressSpecification(storageDir));
//            new StorageAddressSpecifications(
//                            null, null, null, new LocalDiskStorageAddressSpecification(storageDir));

            return this;
        }

        public IbfPersistentStorage build() {
//            if (storageAddress == null) {
//                String storageEnv = OSData.fromEnvRequire(STORAGE_ENV_VAR_NAME);
//                this.storageAddress = parseJsonString(storageEnv);
//            }

            this.storageClient = buildStorageClient();

            return new IbfPersistentStorage(storageClient, secretKey);
        }

        private IbfStorageClient buildStorageClient() {
//            if (storageAddress.gcpSpecification().isPresent()) {
//                return new GcsIbfStorageClient(
//                        StorageUtils.storageService(
//                                storageAddress.gcpSpecification().get().project,
//                                StorageUtils.deserializeCredentials(
//                                        storageAddress.gcpSpecification().get().credentials)),
//                        ROOT,
//                        storageAddress.gcpSpecification().get());
//            } else if (storageAddress.awsSpecification().isPresent()) {
//                return new AwsIbfStorageClient(ROOT, storageAddress.awsSpecification().get());
//            } else
            if (null != storageAddress && storageAddress.localDiskSpecification().isPresent()) {
                return new LocalDiskIbfStorageClient(storageAddress.localDiskSpecification().get());
            } else if (null == storageAddress && StringUtils.equals(System.getProperty("application.mode"), "script-runner")) {
                /**
                 * Note we are getting issue java.lang.UnsupportedOperationException: storage address specification not supported
                 * while running e2e scripts. So , you need to pass additional parameter 'script-runner'
                 */
                File file = new File("IbfLocal.txt");
                Path path = file.toPath();
                System.out.println("Using local ibf storage location " + path);
                LocalDiskStorageAddressSpecification localDiskStorageAddressSpecification = new LocalDiskStorageAddressSpecification(path);

                return new LocalDiskIbfStorageClient(localDiskStorageAddressSpecification);
            } else {
                throw new UnsupportedOperationException("storage address specification not supported");
            }
        }

//        private StorageAddressSpecifications parseJsonString(String jsonStr) {
//            try {
//                return DefaultObjectMapper.JSON.readValue(jsonStr, StorageAddressSpecifications.class);
//            } catch (IOException ex) {
//                throw new RuntimeException("Error while parsing " + STORAGE_ENV_VAR_NAME + " env as JSON", ex);
//            }
//        }
    }

    private byte[] encrypt(ByteBuf body) throws IOException {
        ByteBufInputStream bodyStream = new ByteBufInputStream(body);
        ByteArrayOutputStream encrypted = new ByteArrayOutputStream(body.capacity() + 16);

        Encrypt.encryptWrite(encrypted, secretKey);
        ByteStreams.copy(bodyStream, encrypted);

        return encrypted.toByteArray();
    }

    private ByteBuf decrypt(InputStream in) throws IOException {
        ByteBuf plaintext = Unpooled.buffer(Math.max(in.available() - 16, 16_384));
        ByteBufOutputStream out = new ByteBufOutputStream(plaintext);

        Encrypt.decryptRead(in, secretKey);
        ByteStreams.copy(in, out);

        return plaintext;
    }
}
