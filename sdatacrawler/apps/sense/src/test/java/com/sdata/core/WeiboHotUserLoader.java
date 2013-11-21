package com.sdata.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.lakeside.core.utils.PatternUtils;
import com.nus.next.db.hbase.thrift.HBaseClient;
import com.nus.next.db.hbase.thrift.HBaseClientFactory;
import com.sdata.core.http.HttpPage;
import com.sdata.core.http.HttpPageLoader;
import com.sdata.core.util.JsoupUtils;
import com.sdata.sense.parser.weiboHot.WeiboAPI;

/**
 * @author zhufb
 *
 */
public class WeiboHotUserLoader {

	private static String MAINPAGE = "http://data.weibo.com/top/hot/school";
	private static String table = "weibo_top_users";

	private static List Fashion = Arrays.asList("娱乐","时尚","艺术","动漫","育儿","文学","健康");
	private static List Times = Arrays.asList("体育","财经","传媒","房产","商业","军事","公益");
	private static List Tech = Arrays.asList("汽车","科技","教育","科普");
	
	private static HBaseClient client = HBaseClientFactory.getClientWithCustomSeri("next-2","thot_type");
	public static void main(String[] args) {
//		getCookie();
		if(!client.exists(table)){
			client.createTable(table, "dcf");
		}
		Document doc = fetchPage(MAINPAGE);
//		List<Element>  elements = JsoupUtils.getList(doc, ".hottabs_list a:contains(媒体)");
		List<Element> elements = JsoupUtils.getList(doc, "div.hottabs_list_second[node-type] a:not(.current)");
		for(Element e:elements){
			String url = JsoupUtils.getLink(e, "a");
			Document tdoc = fetchPage(url);
//			String cur = JsoupUtils.getText(tdoc, ".hottabs_list_second a.current");
//			if(!Fashion.contains(cur)&&!Times.contains(cur)&&!Tech.contains(cur)){
//				continue;
//			}
			String page = tdoc.select(".W_pages a:not(.W_btn_a)").last().text();
			Integer maxPage = Integer.valueOf(page);
			for(int i=2;i<=maxPage+1;i++){
				System.out.println("fetch url:"+url);
				List<Map<String, Object>> users = getUsers(tdoc);
				saveUsers(users);
				url = getNextPage(tdoc,i);
				tdoc = fetchPage(url);
			}
		}
	}


	private static Object getUserId( Object value) {
		return WeiboAPI.getInstance().fetchUserId(value.toString());
	}
	private static List<Map<String,Object>> getUsers(Document tdoc){
		List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
		List<Element> list = JsoupUtils.getList(tdoc, ".box_Show_z tr");
		String cur1 = JsoupUtils.getText(tdoc, ".hottabs_list a.current");
		String cur2 = JsoupUtils.getText(tdoc, ".hottabs_list_second a.current");
		for (int s = 1; s < list.size(); s++) {
			Element e = list.get(s);
			Map<String,Object> user = new HashMap<String, Object>();
			user.put("head", JsoupUtils.getLink(e, ".photo_zw"));
			String name = JsoupUtils.getText(e, ".zw_name a");
			user.put("name", name);
			String userUrl = JsoupUtils.getLink(e, ".zw_name a");
			user.put("url", userUrl);
			user.put("id", getUserId(name));
			user.put("follows", JsoupUtils.getText(e, ".times_zw"));
			user.put("category1", cur1);	
			user.put("category2", cur2);
			user.put("category", "科技教育");
			result.add(user);
//			if(Fashion.contains(cur2)){
//				user.put("category", "生活时尚");
//				result.add(user);
//			}else if(Times.contains(cur2)){
//				user.put("category", "时事新闻");
//				result.add(user);
//			}else if(Tech.contains(cur2)){
//				user.put("category", "科技教育");
//				result.add(user);
//			}
		}
		return result;
	}

	private static void saveUsers(List<Map<String, Object>>  users) {
		for(Map<String, Object> user:users){
			Object rk = user.get("id");
			if(rk == null||StringUtils.isEmpty(rk.toString())){
				continue;
			}
			client.save(table, rk, user);
		}
		System.out.println("save user size:"+users.size());
	}
	
	private static String getNextPage(Document tdoc,int page){
		Element first = tdoc.select("a.W_btn_a:contains(下一页)").first();
		String baseUri = first.baseUri();
		if(baseUri.indexOf("?")>=0){
			baseUri = baseUri.substring(0,baseUri.indexOf("?"));
		}
		String absUrl = baseUri.concat("?").concat(first.attr("action-data"));
		return PatternUtils.replaceMatchGroup("page=(\\d)&", absUrl, 1, String.valueOf(page));
	}
	
	private static Document fetchPage(String url) {
		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(url);
		String content =  page.getContentHtml();
		Document doc = Jsoup.parse(content,url);
		return doc;
	}
}
