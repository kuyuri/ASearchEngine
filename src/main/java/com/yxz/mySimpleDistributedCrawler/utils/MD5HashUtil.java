package com.yxz.mySimpleDistributedCrawler.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *  @author Yu   
 */
public class MD5HashUtil {
	
	public static MessageDigest md5;
	
	static {
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
		}
	}
	
	public static String hash(String key) {
		byte[] keyBytes = key.getBytes();
		md5.update(keyBytes);
		byte[] digestBytes = md5.digest();
		StringBuilder hexStr = new StringBuilder();
		for(int i = 0; i < digestBytes.length; i++) {
			int tmp = ((int)digestBytes[i]) & 0xFF;
			if(tmp < 16) {
				hexStr.append("0");
			}
			hexStr.append(Integer.toHexString(tmp));
		}
		return hexStr.toString();
	}

}
