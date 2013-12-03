package com.sdata.live.parser.twitter;

import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONObject;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.core.util.JsoupUtils;
import com.sdata.live.IDBuilder;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.SenseParser;

public class TwitteParser extends SenseParser {

	protected static Logger log = LoggerFactory.getLogger("Sense.TwitteParser");
	private String CURSOR = "scroll_cursor";
	private String UserUrl = "uurl";
	private String USER= "user";
	public TwitteParser(Configuration conf) {
		super(conf);
	}
	
	private boolean hasMore(JSONObject json) {
		return json.getBoolean("has_more_items");
	}

	public String getCurrentStatus(RawContent rc) {
		JSONObject json =JSONObject.fromObject(rc.getContent());
//		if(!hasMore(json)){
//			return null;
//		}
		if(!json.containsKey(CURSOR)){
			return null;
		}
		String id = json.getString(CURSOR);
		if(id.equals("-1")){
			return null;
		}
		return "&".concat(CURSOR).concat("=").concat(id);
	}
	
	private Document getStatusDoc(RawContent rc) {
		JSONObject json =JSONObject.fromObject(rc.getContent());
		if(json ==null||!json.containsKey("items_html")){
			return null;
		}
		String html = json.getString("items_html");
		Document document = DocumentUtils.parseDocument(html,rc.getUrl());
		return document;
	}
	
	@Override
	public ParseResult parseCrawlItem(Configuration conf, RawContent rc,
			SenseCrawlItem item) {
		ParseResult result = new ParseResult();
		Document doc = this.getStatusDoc(rc);
		Elements els = doc.select("li.js-stream-item");
		Iterator<Element> iterator = els.iterator();
		while(iterator.hasNext()){
			Element el = iterator.next();
			String id = el.attr("data-item-id");
			String url = JsoupUtils.getLink(el, "a.tweet-timestamp");
			String uurl = JsoupUtils.getLink(el, "div.stream-item-header a.account-group");
			SenseFetchDatum sfd = new SenseFetchDatum();
			sfd.setId(IDBuilder.build(item, id));
			sfd.setUrl(url);
			sfd.addMetadata(UserUrl, uurl);
			sfd.setCrawlItem(item);
			result.addFetchDatum(sfd);
		}
		return result;
	}

	public SenseFetchDatum parseDatum(SenseFetchDatum datum,Configuration conf, RawContent c){
		//checkUser
		String uurl = datum.getMeta(UserUrl);
		Document document = DocumentUtils.getDocument(uurl);
		if(document == null){
			return null;
		}
		boolean check = conf.getBoolean("checkInSingapore", false);
		if(check&&!isSingapore(datum,document)){
			return null;
		}
		//parser
		datum = super.parseDatum(datum, conf, c);
		Map user =(Map) datum.getMetadata().get(USER);
		String tweets = JsoupUtils.getText(document, ".stats a.js-nav:contains(Tweets) strong");
		String following = JsoupUtils.getText(document, ".stats a.js-nav:contains(Following) strong");
		String followers = JsoupUtils.getText(document, ".stats a.js-nav:contains(Followers) strong");
		if(!StringUtils.isEmpty(tweets)){
			user.put("stact", Integer.valueOf(tweets.replaceAll(",","")));
		}
		if(!StringUtils.isEmpty(following)){
			user.put("frdct", Integer.valueOf(following.replaceAll(",","")));
		}
		if(!StringUtils.isEmpty(followers)){
			user.put("folct", Integer.valueOf(followers.replaceAll(",","")));
		}
		return datum;
	}
	
	private boolean isSingapore(SenseFetchDatum datum,Document document){
		if(datum.getCrawlItem().getParamStr().toLowerCase().contains("singapore")){
			return true;
		}
		if(datum.getCrawlItem().getParamStr().toLowerCase().contains("#")){
			return true;
		}

		if(datum.getCrawlItem().getParamStr().toLowerCase().contains("@")){
			return true;
		}
		
		String text = JsoupUtils.getText(document, ".profile-card-inner .location-and-url");
		if(StringUtils.isEmpty(text)){
			return false;
		}
		if(text.toLowerCase().contains("singapore")){
			return true;
		}
		
		return false;
	}
}
