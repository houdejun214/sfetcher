package com.sdata.core.parser.html.handler.tencent;

import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.handler.IParserHandler;


/**
 * @author zhufb
 *
 */
public class TencentUidHashCodeHandler implements IParserHandler {
	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}
		return data.hashCode();
	}
}
