package com.sdata.core.parser.html.handler.dianping;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.handler.IParserHandler;


/**
 * @author zhufb
 *
 */
public class DianpingReviewDateHandler implements IParserHandler {
	
	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}
		SimpleDateFormat Format = new SimpleDateFormat("yy-MM-dd HH:mm");
		try {
			String string = data.toString();
			int length = string.length();
			if(length<14){
				return data;
			}
			Date date = Format.parse(string.substring(length-14,length));
			return date;
		} catch (Exception e) {
			return data;
		}
	}
}
