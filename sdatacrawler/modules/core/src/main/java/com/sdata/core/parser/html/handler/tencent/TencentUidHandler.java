package com.sdata.core.parser.html.handler.tencent;

import java.util.UUID;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.handler.IParserHandler;

public class TencentUidHandler implements IParserHandler  {

	public Object handle(IParserContext context, Object data) {
		String name = StringUtils.valueOf(data);
		if(StringUtils.isEmpty(name)){
			return null;
		}
		UUID uid = UUIDUtils.getMd5UUID(name);
		return uid;
	}

}
