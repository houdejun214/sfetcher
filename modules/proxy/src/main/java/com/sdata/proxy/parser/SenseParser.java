package com.sdata.proxy.parser;

import java.util.Map;

import org.jsoup.nodes.Document;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.core.RawContent;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.parser.html.DatumParser;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.proxy.Constants;
import com.sdata.proxy.SenseFactory;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;
import com.sdata.proxy.store.SenseStorer;

/**
 * @author zhufb
 *
 */
public abstract class SenseParser extends SdataParser {

	
	public SenseParser(Configuration conf){
		
	}
	
	public SenseStorer getSenseStore(SenseCrawlItem item){
		return SenseFactory.getStorer(item.getCrawlerName());
	}
	
	public abstract ParseResult parseCrawlItem(Configuration conf,RawContent rc,SenseCrawlItem item);
	
	public SenseFetchDatum parseDatum(SenseFetchDatum datum,Configuration conf, RawContent c){
		if(c == null){
			throw new NegligibleException("RawContent is null!");
		}
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			throw new NegligibleException("webpage content is empty!");
		}
		Document doc = DocumentUtils.parseDocument(content,c.getUrl());
		DatumParser parser = new DatumParser(conf,doc);
		parser.addContext(Constants.DATA_PK, datum.getId());
		parser.addContext(Constants.DATA_URL, c.getUrl());
		Map<String, Object> analysis = parser.analysis();
		datum.addAllMetadata(analysis);
		return datum;
	}
	
}
