package com.sdata.component.fetcher;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.mongodb.MongoException;
import com.sdata.component.parser.GothereParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;

/**
 * Fetch address information according to each postcode in Singapore
 * 
 * @author gaoyk
 * 
 */
public class GothereFetcher extends SdataFetcher {
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.GothereFetcher");
	private static final String QUERY_URL = "http://www.gothere.sg/a/search?q=";
	private static final int BEGIN_PCODE = 100000 - 1;
	private static final int END_PCODE = 1000000 - 1;
	private static Integer pCode = new Integer(BEGIN_PCODE);

	public GothereFetcher(Configuration conf, RunState state)
			throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		GothereParser addrParser = new GothereParser(conf, state);
		this.parser = addrParser;
		String curstate = state.getCurrentFetchState();
		if(StringUtils.isNum(curstate)){
			pCode = new Integer(curstate);
		}
	}
	
	@Override
	public List<FetchDatum> fetchDatumList() {
		List<FetchDatum> fetchlist = new ArrayList<FetchDatum>();
		int count = 0;
		while (pCode < END_PCODE) {
			if (count++ > 100){
				break;
			}
			synchronized (pCode) {
				pCode++;
			}
			String link = QUERY_URL + pCode;
			String content = ((GothereParser) parser).download(link);
			if (content == null) {
				throw new RuntimeException("fetch content is empty");
			}
			RawContent c = new RawContent(link, content);
			c.setMetadata(Constants.ADDRESS_POSTCODE, pCode);
			ParseResult parseList = ((GothereParser) parser).parseList(c);
			if (parseList != null)
				fetchlist.addAll(parseList.getFetchList());
		}
		state.setCurrentFetchState(pCode.toString());
		return fetchlist;
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		return datum;
	}

	@Override
	public boolean isComplete() {
		return (pCode == END_PCODE);
	}

}
