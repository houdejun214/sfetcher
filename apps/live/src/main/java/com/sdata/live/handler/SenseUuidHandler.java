package com.sdata.live.handler;

import com.lakeside.core.utils.UUIDUtils;
import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.handler.IParserHandler;
import com.sdata.sense.Constants;

/**
 * @author zhufb
 *
 */
public class SenseUuidHandler implements IParserHandler {

	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}
		Object source = context.getField(Constants.DATA_TAGS_FROM_SOURCE);
		if(source == null){
			throw new RuntimeException("Get data tags from source is null!");
		}
		return UUIDUtils.getMd5UUID(source.toString().concat("_").concat(data.toString()));
	}
}
