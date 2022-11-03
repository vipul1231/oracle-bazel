package com.example.ibf.metrics;

public enum IbfSubPhaseTags {

    // time from start of query till the complete IBF is returned to the connector
    IBF_ENCODE_IBF_QUERY_EXECUTION("ibf_encode_ibf_query_execution"),

    // time for downloading stored IBF from cloud storage
    IBF_IBF_DOWNLOAD_DURATION("ibf_ibf_download_duration"),

    // time for decoding an IBF
    IBF_IBF_DECODE_DURATION("ibf_ibf_decode_duration"),

    // time for uploading new IBF to cloud storage
    IBF_IBF_UPLOAD_DURATION("ibf_ibf_upload_duration"),

    // IBF types
    PERSISTED_IBF("persisted_ibf"),
    LATEST_IBF("latest_ibf"),
    ;

    private final String value;

    IbfSubPhaseTags(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
