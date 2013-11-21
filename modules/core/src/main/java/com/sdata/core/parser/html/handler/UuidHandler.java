package com.sdata.core.parser.html.handler;

import com.lakeside.core.utils.UUIDUtils;
import com.sdata.core.parser.html.context.IParserContext;

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