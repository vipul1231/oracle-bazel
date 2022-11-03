package com.example;

import com.example.crypto.OracleRecordQueue;
import org.junit.Assert;
import org.junit.Test;

public class OracleRecordQueueTest {

    @Test
    public void testBasicCipherAdapter() {
        String textToEncrypt = "This is a sample test to encrypt";
        OracleRecordQueue oracleRecordQueue = new OracleRecordQueue();

        String answer = oracleRecordQueue.encryptAndDecrypt(textToEncrypt);
        System.out.println("Answer :" + answer);
        Assert.assertEquals(textToEncrypt, answer);
    }

    @Test
    public void testByteArrayListOnFile() {
        String textToEncrypt = "This is a sample test to encrypt";
        OracleRecordQueue oracleRecordQueue = new OracleRecordQueue();

        String answer = oracleRecordQueue.encryptAndDecryptWithByteArrayListOnFile(textToEncrypt);
        System.out.println("Answer :" + answer);
        Assert.assertEquals(textToEncrypt, answer);
    }
}
