package test;


import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;

import com.lakeside.download.http.HttpPageLoader;
import com.sdata.conf.sites.CrawlConfig;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.future.FutureItemDB;

/**
 * @author zhufb
 *
 */
public class HotUserDataToMysql {

	private static String FILE1 = "output/users.txt";
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		HttpPageLoader.getDefaultPageLoader().download("http://172.29.32.205:8080/whi8per-future/hot/tweets/new?source=weibo&timelineId=408416975987933184");
		
		
		
		CrawlConfigManager configs = CrawlConfigManager.load("future");
		CrawlConfig crawlSite = configs.getCurCrawlSite();
		FutureItemDB crawlItemDB = new FutureItemDB(crawlSite.getConf());
		List<String> flines1 = FileUtils.readLines(new File(FILE1), null);
//		for(String l:flines1){
//			String replaceAll = l.replaceAll(" ", "").trim();
//			replaceAll = replaceAll.substring(1,replaceAll.length() -1);
//			String[] split = replaceAll.split(",",3);
//			Object uid = split[0].split(":")[1];
//			sb.append(",").append(uid);
//		}

		Map<String,Object> map = new HashMap<String, Object>();
		map.put("crawlerName", "weiboFuture");
//		map.put("objectId", 1);
		map.put("priorityScore", 100);
		map.put("status", 0);
		map.put("sourceName", "weibo");
		map.put("tags", "jiangsu");
		for(String l:flines1){
//			String replaceAll = l.replaceAll(" ", "").trim();
//			replaceAll = replaceAll.substring(1,replaceAll.length() -1);
//			String[] split = replaceAll.split(",",3);
//			Object uid = split[0].split(":")[1];
			JSONObject json = new JSONObject();
			json.put("dtf_uid", l);
			map.put("parameters", json.toString());
			crawlItemDB.saveCrawlItem(map);
		}
	}
}
