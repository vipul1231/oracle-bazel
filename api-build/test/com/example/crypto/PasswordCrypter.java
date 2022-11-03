package com.example.crypto;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import java.security.Key;
import java.security.SecureRandom;

public class PasswordCrypter {
    private Key key;

    public PasswordCrypter(String password) {
        try {
            KeyGenerator generator;
            generator = KeyGenerator.getInstance("DES");
            SecureRandom sec = new SecureRandom(password.getBytes());
            generator.init(sec);
            key = generator.generateKey();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public byte[] encrypt(byte[] array) {
        try {
            Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, key);

            return cipher.doFinal(array);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] decrypt(byte[] array) {
        try {
            Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, key);

            return cipher.doFinal(array);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
