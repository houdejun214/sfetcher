package com.sdata.live.parser.html;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Document;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.RawContent;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.html.StrategyHtmlParser;
import com.sdata.core.parser.html.field.Tags;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.live.IDBuilder;
import com.sdata.sense.Constants;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.SenseParser;

/**
 * @author zhufb
 *
 */
public class SenseHtmlParser extends SenseParser{
	
	public SenseHtmlParser(Configuration conf,RunState state) {
		super(conf);
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
		//
		Document doc = DocumentUtils.parseDocument(content,rc.getUrl());
		StrategyHtmlParser parser = new StrategyHtmlParser(conf,doc);
		parser.addContext(Constants.DATA_URL, rc.getUrl());
		Map<Tags, Object> data = parser.analysis();
		this.setDatums(result, data, item);
		this.setCategorys(result, data, item);
		return result;
	}
	
	protected void setDatums(ParseResult result,Map<Tags, Object> data,SenseCrawlItem item){
		List<Object> list =(List<Object>)data.get(Tags.DATUM) ;
		if(list == null||list.size() == 0){
			return;
		}
		Iterator<Object> iterator = list.iterator();
		while(iterator.hasNext()){
			Object o = iterator.next();
			if(o == null||"".equals(o.toString())){
				continue;
			}
			SenseFetchDatum datum = new SenseFetchDatum();
			if(o instanceof String){
				byte[] id = IDBuilder.build(item,o.toString());
				datum.setUrl(o.toString());
				datum.setId(id);
			}else if(o instanceof Map){
				String url = StringUtils.valueOf(((Map) o).get(Constants.DATA_URL));
				byte[] id = IDBuilder.build(item,url);
				datum.setUrl(url);
				datum.setId(id);
				datum.setMetadata((Map)o);
			}
			datum.setCurrent(item.getEntryUrl());
			datum.setCrawlItem(item);
			result.addFetchDatum(datum);
		}
	}
	
	protected void setCategorys(ParseResult result,Map<Tags, Object> data,SenseCrawlItem item){
		result.setCategoryList((List<Object>) data.get(Tags.LINKS));
	}

}
