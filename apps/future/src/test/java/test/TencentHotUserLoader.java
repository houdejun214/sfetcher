package test;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;
import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.util.JsoupUtils;
import com.tencent.weibo.utils.Tencent;

/**
 * @author zhufb
 *
 */
public class TencentHotUserLoader {

	private static Map<String,String> header; 
	private static String MAINPAGE = "http://zhaoren.t.qq.com/rank.php";
	private static String table = "tencent_top_users";
	private static List Fashion = Arrays.asList("娱乐","时尚","动漫","游戏","星座","草根","微博之星");
	private static List Times = Arrays.asList("体育","财经","媒体人","媒体机构","公共名人","政府机构","公务人员");
	private static List Tech = Arrays.asList("汽车","科技","教育");
	private static HBaseClient client = HBaseClientFactory.getClientWithCustomSeri("next-2","thot_type");
	public static void main(String[] args) {
		getCookie();
		if(!client.exists(table)){
			client.createTable(table, "dcf");
		}
		Document doc = fetchPage(MAINPAGE);
		List<Element>  elements = JsoupUtils.getList(doc, ".subTab1 a");
		for(Element e:elements){
			String url = JsoupUtils.getLink(e, "a");
			if(MAINPAGE.equals(url)){
				continue;
			}
			while(!StringUtils.isEmpty(url)){
				System.out.println("fetch url:"+url);
				Document tdoc = fetchPage(url);
				String cur = JsoupUtils.getText(tdoc, ".subTab1 a.cur").trim();
				if(!Fashion.contains(cur)&&!Times.contains(cur)&&!Tech.contains(cur)){
					break;
				}
				List<Map<String, Object>> users = getUsers(tdoc);
				saveUsers(users);
				url = getNextPage(tdoc);
			}
		}
	}

	private static List<Map<String,Object>> getUsers(Document tdoc){
		List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
		List<Element> list = JsoupUtils.getList(tdoc, ".userList");
		String cur = JsoupUtils.getText(tdoc, ".subTab1 a.cur").trim();
		if(!Fashion.contains(cur)&&!Times.contains(cur)&&!Tech.contains(cur)){
			return result;
		}
		for(Element e:list){
			Map<String,Object> user = new HashMap<String, Object>();
			user.put("head", JsoupUtils.getLink(e, ".userPic a img"));
			user.put("name", JsoupUtils.getText(e, ".userName"));
			String userUrl = JsoupUtils.getLink(e, ".userName a");
			user.put("url", userUrl);
			String[] split = userUrl.split("/");
			user.put("id", split[split.length-1]);
			user.put("follows", JsoupUtils.getText(e, ".topData"));
			user.put("tencentcate", cur);
			if(Fashion.contains(cur)){
				user.put("category", "生活时尚");
				result.add(user);
			}else if(Times.contains(cur)){
				user.put("category", "时事新闻");
				result.add(user);
			}else if(Tech.contains(cur)){
				user.put("category", "科技教育");
				result.add(user);
			}
		}
		return result;
	}

	private static void saveUsers(List<Map<String, Object>>  users) {
		for(Map<String, Object> user:users){
			Object rk = user.get("id");
			client.save(table, rk, user);
		}
		System.out.println("save user size:"+users.size());
	}
	
	private static String getNextPage(Document tdoc){
		return JsoupUtils.getLink(tdoc, "a.pageBtn:contains(下一页)");
	}
	
	private static Document fetchPage(String url) {
		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(header,url);
		String content =  page.getContentHtml();
		Document doc = Jsoup.parse(content,url);
		return doc;
	}
	/**
	 * @param args
	*/
	public static void getCookie() {
		header = new HashMap<String, String>();
		int i = 0;
		while(i<3){
			try {
				String cookie = Tencent.login("1929981936", "lmsnext");
				header.put("Cookie", cookie);
				break;
			} catch (Exception e) {
				i++;
				e.printStackTrace();
				try {
					Thread.sleep(30*1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}
}
