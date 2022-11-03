package com.example.ibf;

import io.netty.buffer.ByteBuf;

public class IbfSyncData {
    public final ResizableInvertibleBloomFilter persistedIBF;
    public final int lastRecordCount; // the number of record we synced in the last successful sync

    public IbfSyncData(ResizableInvertibleBloomFilter persistedIBF, int lastRecordCount) {
        this.persistedIBF = persistedIBF;
        this.lastRecordCount = lastRecordCount;
    }

    public int dataSize() {
        // 4 bytes for lastRecordCount
        return persistedIBF.dataSize() + 4;
    }

    @Override
    public String toString() {
        return "{persistedIBF=" + persistedIBF.toString() + ", lastRecordCount=" + lastRecordCount + "}";
    }

    public static class Serializer extends ByteBufSerializer<IbfSyncData> {
        ByteBufSerializer<ResizableInvertibleBloomFilter> ibfSerializer =
                new ResizableInvertibleBloomFilter.Serializer();

        @Override
        public IbfSyncData decode(ByteBuf byteBuf) {
            return new IbfSyncData(ibfSerializer.decode(byteBuf), ByteBufSerializer.int32.decode(byteBuf));
        }

        @Override
        public void encode(IbfSyncData ibfSyncData, ByteBuf byteBuf) {
            ibfSerializer.encode(ibfSyncData.persistedIBF, byteBuf);
            ByteBufSerializer.int32.encode(ibfSyncData.lastRecordCount, byteBuf);
        }


        //        @Override
        public String getName() {
            return "IbfSyncData";
        }
    }
}
