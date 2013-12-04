package com.sdata.live.handler.weibo;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;

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
