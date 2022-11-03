package com.example.crypto;


import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Optional;

public class Encrypt {
    public static final String DEFAULT_ALGORITHM = "AES";
    private static boolean appliedJceFix = false;

    static {
        applyJceFix();
    }

    // TODO delete this once all systems are upgraded
    public static void applyJceFix() {
        if (appliedJceFix) return;
        appliedJceFix = true;
        String javaVersion = System.getProperty("java.version");

        if (isRestrictedCryptography()) {
            //LOG.info("Applying JCE unlimited-strength fix for java " + javaVersion);
            try {
                Field field = Class.forName("javax.crypto.JceSecurity").getDeclaredField("isRestricted");
                field.setAccessible(true);
                Field modifiersField = Field.class.getDeclaredField("modifiers");
                modifiersField.setAccessible(true);
                modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
                field.set(null, Boolean.FALSE);
            } catch (ClassNotFoundException
                    | NoSuchFieldException
                    | SecurityException
                    | IllegalArgumentException
                    | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else {
            //LOG.info("Skipping JCE unlimited-strength fix for java " + javaVersion);
        }
    }

    private static boolean isRestrictedCryptography() {
        String javaVersion = System.getProperty("java.version");
        String strongCryptoByDefault = "1.8.0_161";
        return compareJavaVersions(javaVersion, strongCryptoByDefault) < 0;
    }

    static int compareJavaVersions(String left, String right) {
        String[] lefts = left.split("[^\\d]"), rights = right.split("[^\\d]");

        for (int i = 0; i < lefts.length && i < rights.length; i++) {
            int leftInt = Integer.parseInt(lefts[i]), rightInt = Integer.parseInt(rights[i]);
            int compare = Integer.compare(leftInt, rightInt);

            if (compare != 0) return compare;
        }

        return 0;
    }

    private static final KeyGenerator symKeyGenerator = newKeyGenerator();

    /**
     * Create a new AES-256 key that will never be stored. You can use this key to encrypt ephemeral data, usually a
     * temporary file on local disk. Never store this key in persistent storage like disk, s3 or a database!
     */
    public static SecretKey newEphemeralKey() {
        return symKeyGenerator.generateKey();
    }

    public static SecretKey newDataEncryptionKey() {
        return symKeyGenerator.generateKey();
    }

    private static KeyGenerator newKeyGenerator() {
        try {
            KeyGenerator symKeyGenerator = KeyGenerator.getInstance(DEFAULT_ALGORITHM);

            symKeyGenerator.init(256);

            return symKeyGenerator;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Encrypt an outgoing stream of data
     */
    public static CipherOutputStream encryptWrite(OutputStream out, SecretKey key) throws IOException {
        Cipher cipher = cipher(key, Optional.empty(), false);
        out.write(cipher.getIV());
        return new CipherOutputStream(out, cipher);
    }

    /**
     * Decrypt an incoming stream of data
     */
    public static CipherInputStream decryptRead(InputStream in, SecretKey key) throws IOException {
        byte[] iv = new byte[16];

        if (in.read(iv) != 16) throw new RuntimeException("Read wrong number of bytes");

        return new CipherInputStream(in, cipher(key, Optional.of(iv), true));
    }

    public static Cipher cipherForWrite(SecretKey key) {
        return cipher(key, Optional.empty(), false);
    }

    public static Cipher cipherForRead(SecretKey key, byte[] iv) {
        return cipher(key, Optional.of(iv), true);
    }

    public static Cipher cipher(SecretKey key, Optional<byte[]> ivInit, boolean decrypt) {
        Cipher cipher = null;
        try {
            byte[] iv = ivInit.orElseGet(Encrypt::newInitializationVector);
            assert iv.length == 16 : "Initialization vector should be 16 bytes but was " + iv.length;

            cipher = Cipher.getInstance(DEFAULT_ALGORITHM + "/CBC/PKCS5Padding");
            cipher.init(decrypt ? Cipher.DECRYPT_MODE : Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));

            return cipher;
        } catch (InvalidAlgorithmParameterException
                | NoSuchPaddingException
                | NoSuchAlgorithmException
                | InvalidKeyException e) {

            try {
                if (isRestrictedCryptography()) {
                    Field isRestrictedField =
                            Class.forName("javax.crypto.JceSecurity").getDeclaredField("isRestricted");
                    isRestrictedField.setAccessible(true);
                    //LOG.severe("isRestricted: " + isRestrictedField.getBoolean(null));
                }

                if (cipher != null) {
                    //LOG.severe("blockSize: " + cipher.getBlockSize());

                    Field cryptoPermField = cipher.getClass().getDeclaredField("cryptoPerm");
                    cryptoPermField.setAccessible(true);
                    //LOG.severe("cryptoPerm: " + cryptoPermField.get(cipher));
                }
            } catch (NoSuchFieldException | ClassNotFoundException /*| IllegalAccessException*/ ex) {
                //LOG.log(Level.SEVERE, "There was caught an exception", e);
            }

            throw new RuntimeException("Error constructing cipher from secret key!", e);
        }
    }

    public static byte[] newInitializationVector() {
        byte[] result = new byte[16];
        SecureRandom random = new SecureRandom();

        random.nextBytes(result);

        return result;
    }

//    public static EncryptedSecrets encryptSecrets(Object secrets, DataKey dataKey) {
//        SecretKeySpec secretKey = new SecretKeySpec(dataKey.key, "AES");
//        EncryptedSecrets result = new EncryptedSecrets();
//
//        try {
//            // Write encrypted secrets to a byte array
//            ByteArrayOutputStream encryptSecrets = new ByteArrayOutputStream();
//
//            CipherOutputStream writeSecrets = encryptWrite(encryptSecrets, secretKey);
//            DefaultObjectMapper.JSON.writeValue(writeSecrets, secrets);
//            writeSecrets.close();
//
//            // Return encrypted dataKey, secrets
//            result.setDataKey(dataKey.encryptedKey);
//            result.setSecrets(encryptSecrets.toByteArray());
//
//        } catch (IOException e) {
//            //LOG.log(Level.SEVERE, "Failed to encrypt credentials with master key. ", e);
//            throw new RuntimeException(e);
//        }
//
//        return result;
//    }

//    public static <T> T decryptSecrets(EncryptedSecrets secrets, SecretKey secretKey, Class<T> type) {
//        try {
//            CipherInputStream in = Encrypt.decryptRead(new ByteArrayInputStream(secrets.getSecrets()), secretKey);
//            return DefaultObjectMapper.JSON.readValue(in, type);
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
//    }
}
