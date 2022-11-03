package com.example.crypto;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

public class PasswordCrypterTest {
    private static final byte[] MESSAGE = "Alpacas are awesome!".getBytes();
    private PasswordCrypter[] passwordCrypters;
    private byte[][] encryptedMessages;

    @Before
    public void setUp() {
        passwordCrypters = new PasswordCrypter[]{
                new PasswordCrypter("passwd"),
                new PasswordCrypter("passwd"),
                new PasswordCrypter("otherPasswd")
        };

        encryptedMessages = new byte[passwordCrypters.length][];
        for (int i = 0; i < passwordCrypters.length; i++) {
            encryptedMessages[i] = passwordCrypters[i].encrypt(MESSAGE);
        }
    }

    @Test
    public void testEncrypt() {
        for (byte[] encryptedMessage : encryptedMessages) {
            assertFalse(Arrays.equals(MESSAGE, encryptedMessage));
        }

        assertFalse(Arrays.equals(encryptedMessages[0], encryptedMessages[2]));
        assertFalse(Arrays.equals(encryptedMessages[1], encryptedMessages[2]));
    }

    @Test
    public void testDecrypt() {
        for (int i = 0; i < passwordCrypters.length; i++) {
            assertArrayEquals(MESSAGE, passwordCrypters[i].decrypt(encryptedMessages[i]));
        }

        assertArrayEquals(MESSAGE, passwordCrypters[0].decrypt(encryptedMessages[1]));
        assertArrayEquals(MESSAGE, passwordCrypters[1].decrypt(encryptedMessages[0]));

        try {
            assertFalse(Arrays.equals(MESSAGE, passwordCrypters[0].decrypt(encryptedMessages[2])));
        } catch (Exception e) {
            // Anything goes as long as the above statement is not true.
        }

        try {
            assertFalse(Arrays.equals(MESSAGE, passwordCrypters[2].decrypt(encryptedMessages[1])));
        } catch (Exception e) {
            // Anything goes as long as the above statement is not true.
        }
    }
}
