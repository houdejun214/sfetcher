package com.sdata.core.util;

import com.lakeside.core.utils.StringUtils;

/**
 * some utilities methods about http links
 * 
 * @author houdj
 *
 */
public class LinkUtils {
	
	/**
	 * get the link type.
	 * eg:
	 * www.google.com				return "";
	 * www.google.com/1.html 		return html;
	 * www.google.com/1.jpg 		return jpg
	 * www.google.com/1.jpg?p=23 	return jpg;
	 * @param link
	 * @return
	 */
	public static String getLinkFileType(String link){
		if(StringUtils.isEmpty(link)){
			return "";
		}
		if(link.startsWith("http://")){
			link=link.substring(7);
		}
		int size = link.length();
		int lastLevelPosition = link.lastIndexOf("/");
		if(lastLevelPosition==-1 || lastLevelPosition >= size){
			return "";
		}
		String lastLevelString = link.substring(lastLevelPosition+1);
		int extensionPosition = lastLevelString.lastIndexOf(".");
		if(extensionPosition==-1 || (extensionPosition+1)==lastLevelString.length()){
			return "";
		}
		String extension = lastLevelString.substring(extensionPosition+1).trim();
		int paramPosition = extension.indexOf("?");
		if(paramPosition>-1){
			extension = extension.substring(0,paramPosition);
		}
		return extension;
	}
}
