package com.example.ibf.cloud_storage;

//import com.example.cloud_storage.GoogleStorageAddressSpecification;
//import com.google.cloud.storage.Blob;
//import com.google.cloud.storage.BlobId;
//import com.google.cloud.storage.BlobInfo;
//import com.google.cloud.storage.Storage;
//import java.io.InputStream;
//import java.nio.channels.Channels;
//
//public class GcsIbfStorageClient implements IbfStorageClient {
//    private final Storage storage;
//    private final String root;
//    private final GoogleStorageAddressSpecification googleStorageAddressSpecification;
//
//    public GcsIbfStorageClient(
//            Storage storage, String root, GoogleStorageAddressSpecification googleStorageAddressSpecification) {
//        this.storage = storage;
//        this.root = root;
//        this.googleStorageAddressSpecification = googleStorageAddressSpecification;
//    }
//
//    @Override
//    public InputStream get(String objectId) {
//        Blob blob = storage.get(blobIdOf(objectId));
//        if (blob == null) return null;
//
//        return Channels.newInputStream(blob.reader());
//    }
//
//    @Override
//    public void put(String objectId, byte[] encrypt) {
//        BlobInfo blobInfo = BlobInfo.newBuilder(blobIdOf(objectId)).build();
//
//        storage.create(blobInfo, encrypt);
//    }
//
//    private BlobId blobIdOf(String objectId) {
//        return BlobId.of(googleStorageAddressSpecification.bucket, root + "/" + objectId);
//    }
//}
