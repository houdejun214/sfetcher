package com.sdata.hot.weibo;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.Weibo;
import weibo4j.util.WeiboServer;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.parser.SdataParser;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class WeiboHotParser extends SdataParser{

	protected static Logger log = LoggerFactory.getLogger("Hot.WeiboHotParser");
	private int minutes;
	private String retUrl = "http://www.weibo.com/aj/mblog/info/big?id=%d&page=%d";

	Map<String,String> header = new HashMap<String,String>();
	public WeiboHotParser(Configuration conf){
		this.minutes = conf.getInt("crawl.minutes", 5);
		WeiboServer.init("weiboWord");
		header.put("Cookie", Weibo.getCookie());
	}
	
	public void parseList(FetchDispatch dispatch,JSONObject hotTweet) {
		Long id = hotTweet.getLong("id");
		String strPubTime = hotTweet.getString("created_at");
		Date pubTime =(Date) DateFormat.changeStrToDate(strPubTime);
		Date maxTime =DateTimeUtils.add(pubTime, Calendar.MINUTE, minutes);
		FetchDatum hotDatum = new FetchDatum();
		
		Map<String, Object> hotReltionMap = getHotReltionMap(hotTweet);
		hotReltionMap.put("pub_time", pubTime.getTime());
		hotReltionMap.put("id", id);
		
		JSONObject json = this.getRetJson(id, 1);
		int maxPage = this.getMaxPage(json);
		boolean end = false;
		for(int i= maxPage;i>0;i--){
			JSONObject retJson = this.getRetJson(id, i);
			String[] statusIds = this.getStatusIds(retJson);
			if(statusIds==null||statusIds.length==0){
				break;
			}
			List<FetchDatum> list = new ArrayList<FetchDatum>();
			for(String sid:statusIds){
				JSONObject next = WeiboAPI.getInstance().fetchOneTweet(sid);
				if(next == null){
					continue;
				}
				String uid = MapUtils.getInterString(next, "user/id");
				String retPub = next.getString("created_at");
				Date retPubTime =(Date) DateFormat.changeStrToDate(retPub);
				Long retPubLong = retPubTime.getTime();
				// hot reltion
				hotReltionMap.put(sid, uid.concat(",").concat(String.valueOf(retPubLong)));
				// ret datum
				FetchDatum datum = new FetchDatum();
				datum.setMetadata(next);
				list.add(datum);
				if(retPubTime.after(maxTime)){
					end = true;
				}
			}
			log.warn("page:"+i+",id："+id +",Got hot retweets size：" + list.size());
			//分发
			dispatch.dispatch(list);
			if(end){
				break;
			}
		}
		hotTweet.put("rets", hotReltionMap);
		hotDatum.setMetadata(hotTweet);
		dispatch.dispatch(hotDatum);
	}
	
	private JSONObject getRetJson(Long id,Integer page){
//		DocumentUtils.wait(5);
		String url = String.format(retUrl, id,page);
		RawContent r = HotUtils.getRawContent(url,header);
		return JSONObject.fromObject(r.getContent());
	}
	
	
	private int getMaxPage(JSONObject json){
		if(json == null){
			return 0;
		}
		return json.getJSONObject("data").getJSONObject("page").getInt("totalpage");
	}
	
	private String[] getStatusIds(JSONObject json) {
		if(json==null||!json.containsKey("data")){
			return null;
		}
		String html = json.getJSONObject("data").getString("html");
		return getAllMatchPattern(" mid=\"(\\d*)\"", html);
	}

	private Map<String,Object> getHotReltionMap(Map<String,Object> map) {
		Map<String,Object> retReltion = new HashMap<String,Object>();
		retReltion.put("uid", MapUtils.getInter(map, "user/id"));
		retReltion.put("uname", MapUtils.getInter(map, "user/name"));
		return retReltion;
	
	}
	
	private String[] getAllMatchPattern(String regex,String input){
		Pattern pat = PatternUtils.getPattern(regex);
		Matcher matcher = pat.matcher(input);
		ArrayList result = new ArrayList();
		while(matcher.find()){
			for(int i=0;i<matcher.groupCount();i++){
				result.add(matcher.group(i+1));
			}
		}
		return (String[]) result.toArray(new String[result.size()]);
	}
	
	protected Document getDocFromStr(String str) throws DocumentException{
		 return DocumentHelper.parseText(str);  
	}
}
