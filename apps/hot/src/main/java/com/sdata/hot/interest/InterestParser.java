package com.sdata.hot.interest;

import java.util.Date;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.hot.Hot;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class InterestParser extends SdataParser{
	
	private int interestCount;
	
	public InterestParser(Configuration conf){
		this.interestCount = conf.getInt("crawl.count", 3);
	}
	
	@Override
	public ParseResult parseList(RawContent c) {
		String xpath = "//rss/channel/item";
		ParseResult result = new ParseResult();
		if(c == null){
			throw new NegligibleException("RawContent is null!");
		}
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			throw new NegligibleException("init web content is empty!");
		}
		Date fetTime = new Date();
		try {
			Document doc = getDocFromStr(content);
			for(int i=1;i<=interestCount;i++){
				FetchDatum datum = new FetchDatum();
				String prefix = xpath+"[" +i+"]";
				Object title = doc.selectSingleNode(prefix+"/title").getText();
				Object pubDate = doc.selectSingleNode(prefix+"/pubDate").getText();
				Object image = doc.selectSingleNode(prefix+"/ht:picture").getText();
				Object contet = doc.selectSingleNode(prefix+"/ht:news_item[1]/ht:news_item_title").getText();
				Object target = doc.selectSingleNode(prefix+"/ht:news_item[1]/ht:news_item_url").getText();
				byte[] rk = HotUtils.getRowkey(Hot.Interest.getValue(), fetTime, i);
				datum.addMetadata("rk", rk);
				datum.addMetadata("rank", i);
				datum.addMetadata("id", target.hashCode());
				datum.addMetadata("type", Hot.Interest.getValue());
				datum.addMetadata("fet_time", fetTime);
				datum.addMetadata("target", target);
				datum.addMetadata("title", title);
				datum.addMetadata("content", contet);
				datum.addMetadata("image", image);
				datum.addMetadata("pub_time", DateFormat.strToDate(pubDate));
				datum.setUrl(target.toString());
				result.addFetchDatum(datum);
			}
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	protected Document getDocFromStr(String str) throws DocumentException{
		 return DocumentHelper.parseText(str);  
	}
}
