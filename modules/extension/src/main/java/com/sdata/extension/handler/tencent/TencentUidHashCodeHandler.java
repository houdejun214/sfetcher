package com.sdata.extension.handler.tencent;

import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;


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
