package com.sdata.extension.handler.flickr;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;

public class FlickrUidHandler implements IParserHandler  {

	public Object handle(IParserContext context, Object data) {
		String reful = StringUtils.valueOf(data);
		Long uid = null;
		if(StringUtils.isNotEmpty(reful)){
			try {
				String[] sps = reful.split("/");
				for(String sp:sps){
					if(sp.indexOf("@N")>-1){
						uid =Long.valueOf(sp.replace("@N", "01"));
						break;
					}
				}
			} catch (Exception e) {
			}
		}
		return uid;
	}

}
