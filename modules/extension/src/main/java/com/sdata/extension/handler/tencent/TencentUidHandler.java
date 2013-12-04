package com.sdata.extension.handler.tencent;

import java.util.UUID;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;

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
