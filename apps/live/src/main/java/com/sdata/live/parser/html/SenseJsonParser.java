package com.sdata.live.parser.html;

import java.util.Map;

import net.sf.json.JSONObject;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.html.StrategyJSONParser;
import com.sdata.core.parser.html.field.Tags;
import com.sdata.sense.Constants;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseJsonParser extends SenseHtmlParser{

	public SenseJsonParser(Configuration conf,RunState state) {
		super(conf,state);
	}
	
	@Override
	public ParseResult parseCrawlItem(Configuration conf,RawContent rc,SenseCrawlItem item) {
		ParseResult result = new ParseResult();
		if(rc == null){
			throw new NegligibleException("RawContent is null!");
		}
		String content = rc.getContent();
		if(StringUtils.isEmpty(content)){
			throw new NegligibleException("init web content is empty!");
		}
		// if not JSON format it's html document 
		if(!(content.startsWith("{")&&content.endsWith("}"))){
			return super.parseCrawlItem(conf, rc, item);
		}
		JSONObject json = JSONObject.fromObject(content);
		StrategyJSONParser parser = new StrategyJSONParser(conf,json);
		parser.addContext(Constants.DATA_URL, rc.getUrl());
		Map<Tags, Object> data = parser.analysis();
		super.setDatums(result, data, item);
		super.setCategorys(result, data, item);
		return result;
	}
	
}