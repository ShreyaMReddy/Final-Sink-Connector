package com.example.security;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Optional encryption utility that can be used to encrypt sensitive configuration data.
 * This class doesn't modify any existing functionality but provides additional security features.
 */
public class EncryptionUtil {
    private static final Logger LOG = LoggerFactory.getLogger(EncryptionUtil.class);
    private final boolean enabled;
    private final SecretKey key;
    private final String algorithm;
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 16;

    public EncryptionUtil(Config config) {
        this.enabled = config.getBoolean("encryption.enabled");
        this.algorithm = config.getString("encryption.algorithm");
        
        if (enabled) {
            String keyStr = config.getString("encryption.key");
            this.key = new SecretKeySpec(keyStr.getBytes(StandardCharsets.UTF_8), "AES");
        } else {
            this.key = null;
        }
    }

    public String encrypt(String data) {
        if (!enabled) return data;
        
        try {
            Cipher cipher = Cipher.getInstance(algorithm);
            byte[] iv = new byte[GCM_IV_LENGTH];
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);
            byte[] encrypted = cipher.doFinal(data.getBytes());
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            LOG.error("Encryption failed", e);
            return data;
        }
    }

    public String decrypt(String encryptedData) {
        if (!enabled) return encryptedData;
        
        try {
            Cipher cipher = Cipher.getInstance(algorithm);
            byte[] decoded = Base64.getDecoder().decode(encryptedData);
            byte[] iv = new byte[GCM_IV_LENGTH];
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
            return new String(cipher.doFinal(decoded));
        } catch (Exception e) {
            LOG.error("Decryption failed", e);
            return encryptedData;
        }
    }
}
