package com.example.crypto;

import java.nio.ByteBuffer;

public class ElementImpl {

    public ElementImpl() {
    }

    public ElementImpl(byte[] data) {
        this.data = data;
    }

    byte[] data;

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setByteBuffer(ByteBuffer b) {
        data = b.array();
    }
}
