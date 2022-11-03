package com.example.ibf.db_data_validator;

import com.example.snowflakecritic.ibf.StrataEstimator;
import com.example.ibf.StrataEstimatorDecodeResult;

import java.sql.SQLException;

public class IbfPrimaryKeyValidator {
    private final IbfPrimaryKeyEncoder source;
    private final IbfPrimaryKeyEncoder destination;

    private StrataEstimatorDecodeResult result;

    public IbfPrimaryKeyValidator(IbfPrimaryKeyEncoder source, IbfPrimaryKeyEncoder destination) {
        this.source = source;
        this.destination = destination;
    }

    public StrataEstimatorDecodeResult estimateDiff() throws SQLException {
        StrataEstimator sourceSE = source.getPrimaryKeyStrataEstimator();
        StrataEstimator destinationSE = destination.getPrimaryKeyStrataEstimator();

        result = StrataEstimator.estimateDifference(sourceSE, destinationSE);

        return result;
    }
}
