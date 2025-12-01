package com.asiainfo.metrics.common.util;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * AES算法包
 *
 * @author goat-eat-meat
 * @date 2015-10-25
 */
public class AesCipher {
    interface Crypto {
        public static String DEFAULT_AES_IV_KEY = "7b51fd7053196308";
        public static String DEFAULT_SECRET_KEY = "b6fa92796c6431c5";
    }
    static class CipherHelper {
        public static String fillChar(String key,int minLen) {
            if (key == null || key.length()<8) {
                return Crypto.DEFAULT_SECRET_KEY;
            }
            int leng = key.length();

            if (leng < minLen) {
                for (int i = leng; i < minLen; i++) {
                    key += "m";
                }
                return key;
            }
            return key;
        }
    }

    private static Logger LOG  = LoggerFactory.getLogger(AesCipher.class);
    /*
     *
        算法/模式/填充                16字节加密后数据长度        不满16字节加密后长度
        AES/CBC/NoPadding             16                          不支持
        AES/CBC/PKCS5Padding          32                          16
        AES/CBC/ISO10126Padding       32                          16
        AES/CFB/NoPadding             16                          原始数据长度
        AES/CFB/PKCS5Padding          32                          16
        AES/CFB/ISO10126Padding       32                          16
        AES/ECB/NoPadding             16                          不支持
        AES/ECB/PKCS5Padding          32                          16
        AES/ECB/ISO10126Padding       32                          16
        AES/OFB/NoPadding             16                          原始数据长度
        AES/OFB/PKCS5Padding          32                          16
        AES/OFB/ISO10126Padding       32                          16
        AES/PCBC/NoPadding            16                          不支持
        AES/PCBC/PKCS5Padding         32                          16
        AES/PCBC/ISO10126Padding      32                          16
     *
     */
    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";

    private static byte[] ivValue = null;

    static {
        ivValue = Crypto.DEFAULT_AES_IV_KEY.getBytes(StandardCharsets.UTF_8);
    }
    private static final IvParameterSpec IV_SPEC = new IvParameterSpec(ivValue);


    /**
     * 采用默认的key加密数据
     * @param message
     * @return
     */
    public static String encrypt(String message){
        return encrypt(message, Crypto.DEFAULT_SECRET_KEY);
    }

    /**
     * 通过key值加密数据
     * @param message
     * @param key
     * @return 加密后的数据
     */
    public static String encrypt(String message,String key){
        return encrypt(message,new SecretKeySpec(CipherHelper.fillChar(key, 16).getBytes(StandardCharsets.UTF_8), "AES"));
    }

    public static String encrypt(String data,String key,String iv){
        return encrypt(data,ALGORITHM,new SecretKeySpec(CipherHelper.fillChar(key, 16).getBytes(StandardCharsets.UTF_8), "AES"),new IvParameterSpec(iv.getBytes(StandardCharsets.UTF_8)));

    }

    public static String encrypt(String data,SecretKeySpec keySpec){
        return encrypt(data,ALGORITHM,keySpec,IV_SPEC);
    }

    /**
     * 根据算法模式，key，iv信息加密数据
     * @param message 信息
     * @param algorithm 算法
     * @param keySpec  key
     * @param ivSpec iv信息
     * @return
     */
    public static String encrypt(String message,String algorithm,SecretKeySpec keySpec,IvParameterSpec ivSpec){
        try {
            Cipher c = Cipher.getInstance(algorithm);
            if(ivSpec!=null){
                c.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
            }else{
                c.init(Cipher.ENCRYPT_MODE, keySpec);
            }
            byte[] encVal = c.doFinal(message.getBytes(StandardCharsets.UTF_8));
            return Base64.encodeBase64String(encVal);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException |
                 InvalidKeyException | InvalidAlgorithmParameterException | IllegalBlockSizeException |
                 BadPaddingException e) {
            LOG.warn(e.getMessage());
        }
        return message;
    }

    /**
     * 采用默认key解密数据
     * @param encryptedData 待解密的数据
     * @return 解密后的结果
     */
    public static String decrypt(String encryptedData) {
        return decrypt(encryptedData, Crypto.DEFAULT_SECRET_KEY);
    }

    /**
     * 采用指定key解密数据
     * @param encryptedData 待解密的数据
     * @param key 密钥
     * @return 解密后的结果
     */
    public static String decrypt(String encryptedData, String key) {
        return decrypt(encryptedData,new SecretKeySpec(CipherHelper.fillChar(key, 16).getBytes(StandardCharsets.UTF_8), "AES"));
    }

    public static String decrypt(String encryptedData, String key ,String iv) {
        return decrypt(encryptedData,ALGORITHM,new SecretKeySpec(CipherHelper.fillChar(key, 16).getBytes(StandardCharsets.UTF_8), "AES"),new IvParameterSpec(iv.getBytes(StandardCharsets.UTF_8)));
    }

    public static String decrypt(String data,SecretKeySpec keySpec){
        return decrypt(data,ALGORITHM,keySpec,IV_SPEC);
    }

    /**
     * 根据算法模式，key，iv信息解密数据
     * @param encryptedData 待解密的内容
     * @param algorithm 算法
     * @param keySpec  key
     * @param ivSpec iv信息
     * @return
     */
    public static String decrypt(String encryptedData, String algorithm,SecretKeySpec keySpec,IvParameterSpec ivSpec) {
        if(encryptedData==null || encryptedData.isEmpty()){
            return "";
        }
        try {
            Cipher c = Cipher.getInstance(algorithm);
            if(ivSpec!=null){
                c.init(Cipher.DECRYPT_MODE, keySpec,ivSpec);
            }else{
                c.init(Cipher.DECRYPT_MODE, keySpec);
            }
            byte[] decordedValue = Base64.decodeBase64(encryptedData);
            byte[] decValue = c.doFinal(decordedValue);
            return new String(decValue, StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException e) {
            LOG.error(e.getMessage());
        } catch (NoSuchPaddingException e) {
            LOG.error(e.getMessage());
        } catch (InvalidKeyException e) {
            LOG.error(e.getMessage());
        } catch (InvalidAlgorithmParameterException e) {
            LOG.error(e.getMessage());
        } catch (IllegalBlockSizeException e) {
//			LOG.error(e.getMessage(),e);
            LOG.warn(e.getMessage());
        } catch (BadPaddingException e) {
            LOG.error(e.getMessage());
        }
        return encryptedData;
    }

}
