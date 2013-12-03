package test;

import java.io.IOException;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.util.EncodingUtil;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * @author zhufb
 *
 */
public class PInterestTest {
	
	private static HttpClient httpClient = new HttpClient(new SimpleHttpConnectionManager(true));
	private static String PIN_URL = "http://www.pinterest.com/pin/{0}/";
	private static String BOARD_URL = "http://www.pinterest.com/resource/BoardFeedResource/get/?source_url={0}&data={1}";
	
	public static String fetchPinInfo(String pinId) throws HttpException, IOException{
		String url = MessageFormat.format(PIN_URL, pinId);
		return fetchContent(url);
	}
	
	public static String fetchContent(String url) throws HttpException, IOException{
		return fetchContent(url,null);
	}
	
	public static String fetchContent(String url,Map<String,String> header) throws HttpException, IOException{
		GetMethod get = new GetMethod(url);
		if(header != null){
			Iterator<String> iterator = header.keySet().iterator();
			while(iterator.hasNext()){
				String k = iterator.next();
				String v = header.get(k);
				get.addRequestHeader(k,v);
			}
    	}
	    httpClient.executeMethod(get);
		return EncodingUtil.getString(get.getResponseBody(),get.getResponseCharSet());
	}
	
	/*
	 *  fetch board info   
	 */
	public static String fetchBoard(String sourceUrl,String boardId,String bookmarks) throws HttpException, IOException{
		JSONObject options = new JSONObject();
    	options.put("board_id", boardId);
    	if(!StringUtils.isEmpty(bookmarks)){
    		options.put("bookmarks",new String[]{bookmarks});
    	}
    	JSONObject data = new JSONObject();
    	data.put("options", options);
    	String dataEncode = URLEncoder.encode(data.toString(),"UTF-8");
    	String url = MessageFormat.format(BOARD_URL, sourceUrl,dataEncode);
    	Map<String,String> header = new HashMap<String,String>();
    	header.put("User-Agent","Mozilla/5.0 (Windows; U; Windows NT 6.1; pl; rv:1.9.1) Gecko/20090624 Firefox/3.5 (.NET CLR 3.5.30729)");
    	header.put("Content-Type","text/html; charset=utf-8");
    	header.put("Connection", "close");
		header.put("X-CSRFToken", "xjZapdseFW3sbWsc3tHN6Ipm4XMgXaOf");
		header.put("X-NEW-APP", "1");
		header.put("X-Requested-With", "XMLHttpRequest");
    	return fetchContent(url,header);
	}
	
	/*
	 * Get book mark and base url. Jsoup can only parse html 
	 */
	public static String nextBookmarks(String content){
		JSONObject jo = JSONObject.fromObject(content);
		//get bookmarks
		String bookmark  = null;
		try{
			bookmark = jo.getJSONObject("resource").getJSONObject("options").getJSONArray("bookmarks").getString(0);
		}catch(Exception e){
			e.printStackTrace();
		}
		return bookmark;
	}
	
	public static String getMatchPattern(String regex,String input,int groupIndex){
		Pattern pat = Pattern.compile(regex);
		Matcher matcher = pat.matcher(input);
		if(matcher.find()){
			return matcher.group(groupIndex);
		}
		return "";
	}

	public static void main(String[] args) throws Exception { 
		
    	// Get Pin info 
		String pinId = "443323157040530104";
    	String html = fetchPinInfo(pinId);
    	Document document = Jsoup.parse(html);
    	
    	String sourceUrl = document.select(".boardHeader a").attr("href");
    	String script = document.select("script").html().replaceAll("\n", "").replaceAll("\n", "").replaceAll(" ", "");
    	String boardId = getMatchPattern("\"board_id\":\"(\\d*)\"", script, 1);
    	
    	sourceUrl = URLEncoder.encode(sourceUrl, "UTF-8");
    	
    	String bookmarks = null;
    	while(true){
        	// first page
        	String content = fetchBoard(sourceUrl,boardId,bookmarks);
        	
        	//TODO get pin info
        	
        	bookmarks = nextBookmarks(content);
        	
        	if(StringUtils.isEmpty(bookmarks)){
        		break;
        	}
    	}
    	
    }
}
