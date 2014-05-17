package com.sdata.component.data.dao;

import java.io.UnsupportedEncodingException;

public class MessyFixer {

	/**
	 * 解决乱码问题
	 * @param oldStr
	 * @return
	 * @throws UnsupportedEncodingException 
	 */
	public static String convertContent(String oldStr) throws UnsupportedEncodingException{
		oldStr = new String(oldStr.getBytes("UTF-8"),"UTF-8");
		 StringBuilder sb =new StringBuilder();
		 for(int i=0;i<oldStr.length();i++){
			 char ch = oldStr.charAt(i);
			 if(Character.isUnicodeIdentifierPart(ch) || validUTF8(ch)){
				 sb.append(ch);
			 }else{
				 sb.append("\\u"+ Integer.toHexString(ch) );
			 }
		 }
		 return sb.toString();
	}
	
	
	public static void main(String[] args) throws UnsupportedEncodingException{
		String test = "1234567890";
		test = "😷不知道禽流感啊？";
		System.out.println(convertContent(test));
		test = "淏歌我正忙着抓小鸡！😷不知道禽流感啊？！";
		System.out.println(convertContent(test));
		test = "，。？";
		System.out.println(convertContent(test));
		test = "~！@#￥%……&*（）——+-";
		System.out.println(convertContent(test));
		
		test = "➤ Google Maps for iPhone (iTunes, free)";
		System.out.println(convertContent(test));
	}
	
	public static boolean validUTF8(char input){
		byte[] bytes = new byte[]{(byte)(input >> 8), (byte) input};
		byte b1 = bytes[0];
		byte b2 = bytes[1];
		if ((b1 & 0x80) == 0 || (b1 & 0xf0)== 0xf0) {
			return true;
		}else if ((b1 & 0xE0) == 0xC0 && (b2 & 0xC0) == 0x80 ) {
			return true;
		}else{
			System.out.print("\n["+input+":"+Integer.toHexString(input)+"]");
			return false;
		}
	}
}