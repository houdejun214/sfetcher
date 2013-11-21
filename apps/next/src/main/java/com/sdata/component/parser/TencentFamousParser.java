package com.sdata.component.parser;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;


public class TencentFamousParser extends SdataParser{
	
	
	public static final Log log = LogFactory.getLog("SdataCrawler.TencentFamousParser");
	
	public TencentFamousParser(Configuration conf,RunState state){
		setConf(conf);
	}

	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			return result;
		}
		Document doc = parseHtmlDocument(content);
		if(doc == null) {
			return result;
		}else{
			Elements famousList = doc.select(".userList");
			Iterator<Element> famousIterator = famousList.iterator();
			while(famousIterator.hasNext()){
				Element famous = famousIterator.next();
				FetchDatum datum = new FetchDatum();
				// fetch order
				String order = famous.select(".ico_num").first().html();
				//fetch user's url and user image's url
				String imgUrl = null;
				String userUrl = null;
				Element epic = famous.select(".userPic").first();
				Element elea = epic.select("a").first();
				userUrl = elea.attr("href");
				Element pic = elea.select("img").first();
				if(pic!=null){
					imgUrl = pic.attr("src");
				}
				//fetch user's nick
				Element eleun =  famous.select(".userName").first();
				Element una = eleun.select("a").first();
				String nick =una.html();
				if(StringUtils.isEmpty(userUrl)){
					userUrl = una.attr("href");
				}
				//fetch name
				String userName = null;
				if(StringUtils.isNotEmpty(userUrl)){
					String[] splits = userUrl.split("/");
					userName = splits[splits.length-1];
				}
				//fetch topData
				String topData = famous.select(".topData").first().html();
				//fetch pint
				String pint = famous.select(".pint").first().html();
				datum.addMetadata(Constants.TENCENT_FAMOUS_ORDER, Integer.parseInt(order));
				datum.addMetadata(Constants.TENCENT_FAMOUS_NAME, userName);
				datum.addMetadata(Constants.TENCENT_FAMOUS_USERURL, userUrl);
				datum.addMetadata(Constants.TENCENT_FAMOUS_HEAD, imgUrl);
				datum.addMetadata(Constants.TENCENT_FAMOUS_NICK, nick);
				datum.addMetadata(Constants.FAMOUS_POPULAR, topData);
				datum.addMetadata(Constants.TENCENT_FAMOUS_PRINT, pint);
				datum.addMetadata(Constants.UID,userName.hashCode());
				datum.setName(userName);
				result.addFetchDatum(datum);
			}
		}
		return result;
	}


	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			return result;
		}
		return result;
	}
	
}
