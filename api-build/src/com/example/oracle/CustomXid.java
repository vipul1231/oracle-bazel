package com.example.oracle;

import javax.transaction.xa.Xid;

/**
 * @TODO: Need to write business logic
 */
public class CustomXid implements Xid {

    public CustomXid(byte[] xidRaw) {
    }

    @Override
    public int getFormatId() {
        return 0;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return new byte[0];
    }

    @Override
    public byte[] getBranchQualifier() {
        return new byte[0];
    }
}
