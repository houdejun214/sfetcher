package com.sdata.hot.social;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import twitter4j.Status;
import twitter4j.Trend;
import twitter4j.Trends;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.hot.Hot;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class SocialParser extends SdataParser{
	
	private int count;
	private int woeid;
	private TwitterApi api;
	
	public SocialParser(Configuration conf){
		this.woeid = conf.getInt("woeid", 23424948);
		this.count = conf.getInt("crawl.count", 3);
		this.api = new TwitterApi(conf);
	}
	
	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		Trends trends = api.trends(woeid);
		Trend[] trends2 = trends.getTrends();
		Date fetTime = new Date();
		for(int i=0;result.getFetchList().size()<count&&i<trends2.length;i++){
			FetchDatum datum = new FetchDatum();
			Trend trend = trends2[i];
			int rank = result.getFetchList().size()+1;
			byte[] rk = HotUtils.getRowkey(Hot.Social.getValue(), fetTime, rank);
			datum.addMetadata("rk", rk);
			datum.addMetadata("rank", rank);
			datum.addMetadata("type", Hot.Social.getValue());
			datum.addMetadata("fet_time", fetTime);
			datum.addMetadata("pub_time", trends.getTrendAt());
			datum.addMetadata("id", trend.getName().hashCode());
			datum.addMetadata("title", trend.getName());
			datum.addMetadata("target", trend.getURL());
			//image
			String str = api.searchByUrl(trend.getQuery());
			String imageUrl = getImageUrl(str);
			if(StringUtils.isEmpty(imageUrl)){
				continue;
			}
			datum.addMetadata("image", imageUrl);
			datum.setName(trend.getQuery());
			datum.setUrl(trend.getURL());
			result.addFetchDatum(datum);
		}
		return result;
	}

	public void parse(FetchDatum datum) {
		if(datum== null||StringUtils.isEmpty(datum.getName())){
			throw new NegligibleException("social parser datum name is null!");
		}
		// tweets
		List<Status> list = api.search(datum.getName());
		if(list==null||list.size()<count){
			list = api.searchRecent(datum.getName());
		}
		List<Map<String,Object>> tweets = new ArrayList<Map<String,Object>>();
		for(int i=0;i<count&&i<list.size();i++){
			Status s = list.get(i);
			Map<String,Object> tw = new HashMap<String,Object>();
			tw.put("id", s.getId());
			tw.put("pub_time", s.getCreatedAt());
			tw.put("content", s.getText());
			if(s.getGeoLocation()!=null){
				tw.put("geo", s.getGeoLocation().getLatitude()+","+s.getGeoLocation().getLongitude());
			}
			tw.put("retct", s.getRetweetCount());
			tw.put("uname", s.getUser().getName());
			tw.put("sname", s.getUser().getScreenName());
			tw.put("uid", s.getUser().getId());
			tw.put("head", s.getUser().getProfileImageURL());
			tw.put("uurl", s.getUser().getURL());
			tweets.add(tw);
		}
		datum.addMetadata("content", tweets);
		
	}
	
	private String getImageUrl(String content) {
		Document doc = this.getImageDoc(content);
		Element el = doc.select("span[data-url],a[data-url]").first();
		if(el == null){
			return null;
		}
		String image = el.attr("data-url");
		if(StringUtils.isEmpty(image)){
			return null;
		}
		return image.replaceAll(":thumb", "");
	}
	
	private Document getImageDoc(String content) {
		JSONObject json =JSONObject.fromObject(content);
		if(json ==null||!json.containsKey("items_html")){
			return null;
		}
		String html = json.getString("items_html");
		Document document = DocumentUtils.parseDocument(html);
		return document;
	}
	
}
