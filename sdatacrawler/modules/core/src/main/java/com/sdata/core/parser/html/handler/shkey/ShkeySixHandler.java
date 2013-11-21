package com.sdata.core.parser.html.handler.shkey;

import com.sdata.core.data.DataShKey;
import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.handler.IParserHandler;


/**
 * @author zhufb
 *
 */
public class ShkeySixHandler implements IParserHandler {
	
	private DataShKey dbShkey = new DataShKey(3);
	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}
		String shKey = dbShkey.getShKey(data.toString());
		try{
			return Long.valueOf(shKey);
		}catch(Exception e){
			return shKey;
		}
	}
}
