package test;

import java.util.HashMap;
import java.util.Map;

import weibo4j.Weibo;

import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;

public class WeiboLoginTest {

	public static void main(String[] args) {
		String cookie = Weibo.getCookie("nextcentre@gmail.com", "nextcentre");
		// String cookie = Weibo.getCookie("2033701349@qq.com","lmsnext");
		System.out.println(cookie);
		String page = "http://www.weibo.com/u/2815446972/home?topnav=1&wvr=5";
		// String page =
		// "http://www.weibo.com/u/3956779699/home?topnav=1&wvr=5";
		Map<String, String> map = new HashMap<String, String>();
		map.put("Cookie", cookie);
		HttpPage download = HttpPageLoader.getDefaultPageLoader().download(map,page);
		System.out.println(download.getContentHtml());
	}
}
