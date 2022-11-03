package com.example.ibf.cloud_storage;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.common.base.Strings;

import java.io.InputStream;

public class AwsIbfStorageClient implements IbfStorageClient {
    private final String root;
//    private final AwsStorageAddressSpecification awsStorageAddressSpecification;
//    private final AmazonS3 sdkClient;
//    private final TransferManager transferManager;

//    public AwsIbfStorageClient(String root, AwsStorageAddressSpecification awsStorageAddressSpecification) {
    public AwsIbfStorageClient(String root) {
        this.root = root;
//        this.awsStorageAddressSpecification = awsStorageAddressSpecification;
//        this.sdkClient = createSdkClient(awsStorageAddressSpecification.region);
//        this.transferManager = createTransferManager(sdkClient);
    }

    @Override
    public InputStream get(String objectId) {
        S3Object object;
        try {
//            object = sdkClient.getObject(awsStorageAddressSpecification.bucket, getObjectKey(objectId));
            object = null;
        } catch (AmazonS3Exception ex) {
            if (ex.getErrorCode().equals("NoSuchKey")) return null; // object does not exist

            throw ex;
        }

        return object.getObjectContent();
    }

    @Override
    public void put(String objectId, byte[] encrypt) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(encrypt.length);

//        transferManager.upload(
//                awsStorageAddressSpecification.bucket,
//                getObjectKey(objectId),
//                new ByteArrayInputStream(encrypt),
//                objectMetadata);
    }

    private AmazonS3 createSdkClient(String region) {
//        AWSCredentialsProvider credentialsProvider =
//                buildAWSCredentialsProvider(
//                        awsStorageAddressSpecification.accessKey, awsStorageAddressSpecification.secretKey);

        // Use a default client retry with an exponential backoff.
//        return AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(credentialsProvider).build();
        return null;
    }

    private TransferManager createTransferManager(AmazonS3 sdkClient) {
        return TransferManagerBuilder.standard().withS3Client(sdkClient).build();
    }

    private static AWSCredentialsProvider buildAWSCredentialsProvider(String accessKey, String secretKey) {
        if (!Strings.isNullOrEmpty(accessKey) && !Strings.isNullOrEmpty(secretKey)) {
            return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        }
        // Fallback for local testing
        return new ProfileCredentialsProvider("aws-test");
    }

    private String getObjectKey(String objectId) {
        return root + "/" + objectId;
    }
}
