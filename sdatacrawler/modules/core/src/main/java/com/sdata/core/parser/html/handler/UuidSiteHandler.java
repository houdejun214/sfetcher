package com.sdata.core.parser.html.handler;

import org.apache.commons.lang.StringUtils;

import com.lakeside.core.utils.UUIDUtils;
import com.sdata.core.Constants;
import com.sdata.core.parser.html.context.IParserContext;


/**
 * @author zhufb
 *
 */
public class UuidSiteHandler implements IParserHandler {
	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}
		String site = getSite(context);
		if(StringUtils.isEmpty(site)){
			return data;
		}
		return UUIDUtils.getMd5UUID(site.concat(data.toString()));
	}
	
	protected String getSite(IParserContext context){
		String site = context.getConfig().get(Constants.SOURCE);
		if(StringUtils.isEmpty(site)){
			return null;
		};
		return site;
	}
}
