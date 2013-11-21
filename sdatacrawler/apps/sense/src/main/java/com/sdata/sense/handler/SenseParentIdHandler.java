package com.sdata.sense.handler;

import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.handler.IParserHandler;
import com.sdata.sense.Constants;
import com.sdata.sense.SenseIDBuilder;

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
		return SenseIDBuilder.build(entry, objectId, data);
	}
}
