package com.sdata.common.parser;

import com.lakeside.core.utils.StringUtils;
import com.sdata.common.CommonItem;
import com.sdata.common.IDBuilder;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.RawContent;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.parser.ParseResult;
import com.sdata.proxy.Constants;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;
import com.sdata.proxy.parser.SenseParser;

import de.jetwick.snacktory.HtmlFetcher;
import de.jetwick.snacktory.JResult;

/**
 * @author zhufb
 * 
 */
public class CommonParser extends SenseParser {

	
	public CommonParser(Configuration conf, RunState state) {
		super(conf);
	}

	@Override
	public ParseResult parseCrawlItem(Configuration conf, RawContent rc,
			SenseCrawlItem item) {
		CommonItem citem = (CommonItem)item;
		ParseResult result = new ParseResult();
		if (rc == null) {
			throw new NegligibleException("RawContent is null!");
		}
		String url = rc.getUrl();
		if (StringUtils.isEmpty(url)) {
			throw new NegligibleException("RawContent url is null!");
		}
		// fetch and extract the url
		JResult res = new HtmlFetcher().fetchAndExtract(url);
		if(!res.isArticle()) {
			result.setCategoryList(res.getLinks());
		}else{
			result.addFetchDatum(this.getFetchDatum(res,citem));
		}
		return result;
	}

	protected SenseFetchDatum getFetchDatum(JResult res,
			CommonItem item) {
		SenseFetchDatum datum = new SenseFetchDatum();
		String url = res.getUrl();
		byte[] id = IDBuilder.build(item, url.hashCode());
		datum.addAllMetadata(res.toMap());
		datum.setUrl(url);
		datum.addMetadata(Constants.DATA_ID,id);
		datum.setId(id);
		datum.setCrawlItem(item);
		return datum;
	}

}
