package com.sdata.extension.handler.common;

import com.lakeside.core.utils.UUIDUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;

/**
 * @author zhufb
 *
 */
public class UuidHandler implements IParserHandler {

	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}
		return UUIDUtils.getMd5UUID(data.toString());
	}
}
