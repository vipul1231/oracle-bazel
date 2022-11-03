package com.example.crypto;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;

class ByteBufferOutputStream extends OutputStream {
    List<ByteBuffer> bList = new ArrayList<>();
    ByteBuffer b;
    int len;

    ByteBufferOutputStream(int len) {
        this.b = ByteBuffer.allocate(len);
        b.order(ByteOrder.LITTLE_ENDIAN);
        this.len = len;
        bList.add(b);
    }

    @Override
    public void write(int data) throws IOException {
        if (b.remaining() == 0) {
            b.flip();
            this.b = ByteBuffer.allocate(len / 2);
            b.order(ByteOrder.LITTLE_ENDIAN);
            bList.add(b);
        }
        b.put((byte) data);
    }

    byte[] array() {
        b.flip();
        byte[] newBytes = new byte[getSize()];
        ByteBuffer b1 = ByteBuffer.wrap(newBytes);
        b1.order(ByteOrder.LITTLE_ENDIAN);
        bList.forEach(a -> b1.put(a.array(), 0, a.limit()));
        return newBytes;
    }

    public int getSize() {
        return bList.stream().map(ByteBuffer::limit).reduce(0, Integer::sum);
    }
}

public class BasicCipherAdapter /*implements CipherAdapter*/ {
    final SecretKey secretKey;

    /** This constructor should only be used for ephemeral data. */
    public BasicCipherAdapter() {
        this.secretKey = Encrypt.newEphemeralKey();
    }

    public BasicCipherAdapter(SecretKey secretKey) {
        this.secretKey = secretKey;
    }

//    @Override
    public byte[] encrypt(byte[] bytes) throws Exception {
        if (bytes == null) {
            return new byte[0];
        }
        try {
            ByteBufferOutputStream out = new ByteBufferOutputStream(bytes.length + 100);
            CipherOutputStream eOut = Encrypt.encryptWrite(out, secretKey);
            eOut.write(bytes);
            eOut.close();
            return out.array();
        } catch (IOException e) {
            e.printStackTrace();
//            throw new ByteArrayListException("Encyrpt failed.", e);
            throw new Exception("Decrypt failed.", e);
        }
    }

//    @Override
    public byte[] decrypt(byte[] bytes) throws Exception {
        if (bytes == null) {
            return new byte[0];
        }
        try (ByteArrayInputStream eIn = new ByteArrayInputStream(bytes);
             InputStream in = Encrypt.decryptRead(eIn, secretKey); ) {
            try (ByteBufferOutputStream bOut = new ByteBufferOutputStream(bytes.length)) {
                int c;
                while ((c = in.read()) >= 0) {
                    bOut.write(c);
                }
                return bOut.array();
            }
        } catch (IOException e) {
            e.printStackTrace();
//            throw new ByteArrayListException("Decrypt failed.", e);
            throw new Exception("Decrypt failed.", e);
        }
    }
}
