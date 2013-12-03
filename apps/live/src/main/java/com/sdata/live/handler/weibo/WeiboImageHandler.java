package com.sdata.live.handler.weibo;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.handler.IParserHandler;

/**
 * @author zhufb
 *
 */
public class WeiboImageHandler implements IParserHandler {

	public Object handle(IParserContext context, Object data) {
		if(data == null||StringUtils.isEmpty(data.toString())){
			return null;
		}
		return data.toString().replace("thumbnail", "large");
	}

}
