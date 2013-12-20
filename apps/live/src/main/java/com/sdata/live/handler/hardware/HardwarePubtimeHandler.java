package com.sdata.live.handler.hardware;

import java.util.Date;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;

/**
 * @author zhufb
 *
 */
public class HardwarePubtimeHandler implements IParserHandler {

	public Object handle(IParserContext context, Object data) {
		if(data == null||StringUtils.isEmpty(data.toString())){
			return null;
		}
		String date = data.toString().replace("Today", DateTimeUtils.format(new Date(), "dd-MM-yyyy")).trim();
		return DateFormat.strToDate(date);
	}

}
