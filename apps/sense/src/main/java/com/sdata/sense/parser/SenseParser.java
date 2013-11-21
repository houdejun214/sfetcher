package com.sdata.sense.parser;

import java.util.Map;

import org.jsoup.nodes.Document;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.parser.html.DatumParser;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.sense.Constants;
import com.sdata.sense.SenseFactory;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.store.SenseStorer;

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