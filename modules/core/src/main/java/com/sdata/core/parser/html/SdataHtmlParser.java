package com.sdata.core.parser.html;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.sdata.core.parser.config.DatumConfig;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.util.Documents;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.parser.html.field.Tags;


/**
 * @author zhufb
 *
 */
public class SdataHtmlParser extends SdataParser{
	public static final Log log = LogFactory.getLog("SdataCrawler.SdataHtmlParser");
	public SdataHtmlParser(Configuration conf,RunState state){
		super.setConf(conf);
		super.state = state;
	}
	
	
	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		if(c == null){
			throw new NegligibleException("RawContent is null!");
		}
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			throw new NegligibleException("init web content is empty!");
		}
		Document doc = Documents.parseDocument(content, c.getUrl());
		StrategyHtmlParser parser = new StrategyHtmlParser(getConf(),doc);
        parser.addAllContext(c.getMetadata());
		Map<Tags, Object> data = parser.analysis();
		this.parseCategory(result, data,c);
		this.parseDatum(result, data,c);
        for (Map<String,Object> cate : result.getCategoryList()) {
            cate.putAll(parser.getContext().getReturnContext());
        }
		return result;
	}
	
	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		if(c == null){
			throw new NegligibleException("RawContent is null!");
		}
		if(c.isEmpty() && c.isLazyDownload()){
            boolean needFetchContent = DatumConfig.getInstance(this.getConf()).needFetchContent(c.getUrl());
            if (!needFetchContent) {
                DatumParser parser = new DatumParser(getConf(), new Document(c.getUrl()));
                parser.addAllContext(c.getMetadata());
                parser.addContext("url", c.getUrl());
                Map<String, Object> analysis = parser.analysis();
                result.setMetadata(analysis);
                return result;
            } else {
                c.fetchContent();
            }
        }
        if(c.isEmpty()){
            throw new NegligibleException("webpage content is empty!");
        }
        Document doc = Documents.parseDocument(c.getContent(), c.getUrl());
		DatumParser parser = new DatumParser(getConf(),doc);
        parser.addAllContext(c.getMetadata());
        parser.addContext("url", c.getUrl());
		Map<String, Object> analysis = parser.analysis();
		result.setMetadata(analysis);
		return result;
	}
	
	protected void parseDatum(ParseResult result, Map<Tags, Object> data, RawContent raw){
		List<Object> list =(List<Object>)data.get(Tags.DATUM) ;
		if(list == null||list.size() == 0){
			return;
		}
		String current = raw.getUrl();
		Iterator<Object> iterator = list.iterator();
		while(iterator.hasNext()){
			Object o = iterator.next();
			if(o == null||"".equals(o.toString())){
				continue;
			}
			FetchDatum datum = new FetchDatum();
			if(o instanceof String){
				datum.setUrl(o.toString());
			}else if(o instanceof Map){
				datum.setUrl(StringUtils.valueOf(((Map) o).get("url")));
				datum.setMetadata((Map)o);
			}
			datum.setCurrent(current);
			result.addFetchDatum(datum);
		}
	}

	protected void parseCategory(ParseResult result,Map<Tags, Object> data,RawContent raw){
		List<Object> list = (List<Object>) data.get(Tags.LINKS);
		if(list == null||list.size() == 0){
			return;
		}
        Integer depth = 1;
        String str = StringUtils.valueOf(raw.getMetadata(Constants.QUEUE_DEPTH));
        if (StringUtils.isInt(str)) {
            depth = StringUtils.toInt(str);
        }
		List<Map<String,Object>> _list = new ArrayList<Map<String,Object>>();
		Iterator<Object> iterator = list.iterator();
		while(iterator.hasNext()){
            Object next = iterator.next();
            if (next instanceof String) {
                String url = StringUtils.valueOf(next);
                if(StringUtils.isEmpty(url)){
                    continue;
                }
                Map<String,Object> map = Maps.newHashMap();
                map.put(Constants.QUEUE_URL, url);
                map.put(Constants.QUEUE_DEPTH, depth+1);
                _list.add(map);
            }else if (next instanceof LinkRequest) {
                LinkRequest request = (LinkRequest) next;
                Map<String,Object> map = Maps.newHashMap();
                map.put(Constants.QUEUE_URL, request.getUrl());
                map.put(Constants.QUEUE_HEADER, request.getHeader());
                map.put(Constants.QUEUE_METHOD, request.getMethod());
                _list.add(map);
            }
        }
		result.setCategoryList(_list);
	}
}
