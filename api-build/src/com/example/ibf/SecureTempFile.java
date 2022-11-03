package com.example.ibf;


import com.example.crypto.Encrypt;

import java.io.*;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.crypto.SecretKey;

public class SecureTempFile implements Iterable<String> {
    private String name;
    private SecretKey secretKey;
    private File file;
    private BufferedReader reader;
    private OutputStreamWriter writer;
    private boolean encrypted = true;
    private boolean compressed = true;

    public SecureTempFile(String name, SecretKey secretKey) throws IOException {
        this(name, secretKey, true);
    }

    public SecureTempFile(String name, SecretKey secretKey, boolean compressed) throws IOException {
        this.name = name;
        this.secretKey = secretKey;
        this.compressed = compressed;
        if (compressed) file = File.createTempFile(name, ".gz.aes");
        else file = File.createTempFile(name, ".aes");
        file.deleteOnExit();
    }

    public File getFile() {
        return file;
    }

    public BufferedReader getBufferedReader() throws IOException {
        if (reader != null) return reader;
        InputStream inputStream = new FileInputStream(file);
        if (encrypted) inputStream = Encrypt.decryptRead(inputStream, secretKey);
        if (compressed) inputStream = new GZIPInputStream(inputStream);
        reader = new BufferedReader(new InputStreamReader(inputStream));
        return reader;
    }

    public OutputStreamWriter getWriter() throws IOException {
        if (writer != null) return writer;
        OutputStream outputStream = new FileOutputStream(file.getAbsoluteFile());
        if (encrypted) outputStream = Encrypt.encryptWrite(outputStream, secretKey);
        if (compressed) outputStream = new GZIPOutputStream(outputStream);
        writer = new OutputStreamWriter(outputStream);
        return writer;
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }

        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    @Override
    public Iterator<String> iterator() {
        try {
            reader = getBufferedReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new Iterator<String>() {
            String next;

            {
                try {
                    next = reader.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public String next() {
                if (next != null) {
                    try {
                        String ret = next;
                        next = reader.readLine();
                        if (next == null) {
                            close();
                        }
                        return ret;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return null;
            }
        };
    }

    public boolean isEncrypted() {
        return encrypted;
    }

    public void setEncrypted(boolean encrypted) {
        this.encrypted = encrypted;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }
}
