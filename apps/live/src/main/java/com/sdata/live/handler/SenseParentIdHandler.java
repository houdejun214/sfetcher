package com.sdata.live.handler;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;
import com.sdata.live.IDBuilder;
import com.sdata.proxy.Constants;

/**
 * @author zhufb
 *
 */
public class SenseParentIdHandler implements IParserHandler {

	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}
		Object entry = context.getVariable(Constants.DATA_TAGS_FROM_SOURCE);
		Object objectId = context.getVariable(Constants.DATA_TAGS_FROM_OBJECT_ID);
		return IDBuilder.build((Long)objectId, StringUtils.valueOf(entry), StringUtils.valueOf(data));
	}
}
