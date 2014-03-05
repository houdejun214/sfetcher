package test;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.lang.StringUtils;

import weibo4j.Weibo;

import com.lakeside.core.utils.time.DateTimeUtils;
import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;

public class WeiboSearchValidTest {

	private static HttpClient client = new HttpClient();
	public static void main(String[] args) throws HttpException, IOException {
//		1389841009765
//		{"code":"100000","msg":"","data":{"retcode":"dd83f2e65e65f38db25b25ed164d9d3a"}}
//		Set-Cookie: ULOGIN_IMG=13928858879945; domain=weibo.com; path=/
//		Set-Cookie: ULOGIN_IMG=13928864598667; domain=weibo.com; path=/
//		1392886569367
//		13928864598667
//		1389841009765
//		13928858879945
//		Set-Cookie: ULOGIN_IMG=13928873616883; domain=weibo.com; path=/
//		Set-Cookie: ULOGIN_IMG=1392887271511; domain=weibo.com; path=/
		String cookie = Weibo.getCookie("2033701349@qq.com","lmsnext");
//		String cookie = "login_sid_t=cbba2236ea69bd7a3737b01a6087a1b4; _s_tentry=-; Apache=472611570730.8054.1392803151543; SINAGLOBAL=472611570730.8054.1392803151543; ULV=1392803151557:1:1:1:472611570730.8054.1392803151543:; myuid=2815446972; SUB=Ad%2FDZjCcknez0PBySYwi35mXqWbsIEJMITU9TJsgeUF4Id1h325ODi5Q%2FoN%2FjY%2FOpDnsTCsHRJqSYz7ncA%2B3viU2KM0XF0XRzYsYiLiIkP4m; SUBP=00170a121e2bf140000; UOR=,,login.sina.com.cn; SUE=es%3D6e1e2fbb9778908f023c457bf00f5802%26ev%3Dv1%26es2%3Dbc2598982f8e78b7c7979c8ebf12defd%26rs0%3DqTxRkx3Etbl1inp1igGw%252FvEivlUw8193g0jLfwQ9kzJw4281kYJQ4FSF7sO522RfiB6ZgU9DgYGzf4PshgRs0Wk%252BShXm7KdmAvtekSNJdqxz46Bd4BLqK%252BNnzzPGQK2hLinKpSrAYC3ZwLmzOIGRvhdclI4e2lff24lEKFZIYno%253D%26rv%3D0; SUP=cv%3D1%26bt%3D1392883875%26et%3D1392970275%26d%3Dc909%26i%3Df4f6%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D%26st%3D0%26uid%3D3956779699%26name%3D2033701349%2540qq.com%26nick%3Dsense-04%26fmp%3D%26lcp%3D; SUS=SID-3956779699-1392883875-JA-04g82-f055cc6496522d7c5193d0d2ce0e480c; ALF=1395475875; SSOLoginState=1392883875; ULOGIN_IMG=13928873616883";
		
//		System.out.println(download.getContentHtml());
//		String code = "2px7";
		cookie = downloadValidImage(cookie);
		String code = inputValidCode();
//		Set-Cookie: ULOGIN_IMG=13928857080421; domain=weibo.com; path=/
//		Set-Cookie: ULOGIN_IMG=13928856409971; domain=weibo.com; path=/
		
//		map.put("Referer", "http://s.weibo.com/weibo/%25E8%2583%25A5%25E6%25B8%25A1%25E5%2590%25A7?topnav=1&wvr=5&Refer=top_hot");
		
//		Host: s.weibo.com
//		Connection: keep-alive
//		Content-Length: 39
//		Origin: http://s.weibo.com
//		X-Requested-With: XMLHttpRequest
//		User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.107 Safari/537.36
//		Content-Type: application/x-www-form-urlencoded
//		Accept: */*
//		Referer: http://s.weibo.com/weibo/x
//		Accept-Encoding: gzip,deflate,sdch
//		Accept-Language: en
		Long currentTime = DateTimeUtils.getCurrentTime();
		String url = "http://s.weibo.com/ajax/pincode/verified?__rnd="+currentTime;
		PostMethod post = new PostMethod(url);
//		post.setRequestHeader("Accept", "*/*");
//		post.setRequestHeader("Accept-Language","en");
//		post.setRequestHeader("Accept-Encoding","gzip,deflate,sdch");
		post.setRequestHeader("Cookie", cookie);
		post.setRequestHeader("X-Requested-With", "XMLHttpRequest");
//		post.setRequestHeader("Origin", "http://s.weibo.com");
//		post.setRequestHeader("Content-length", "39");
//		post.setRequestHeader("Host", "s.weibo.com");
//		post.setRequestHeader("Connection", "keep-alive");
		post.setRequestHeader("User-Agent","Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.107 Safari/537.36");
		post.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
		post.setRequestHeader("Referer", "http://s.weibo.com/weibo/x");
		NameValuePair[] nvps = new NameValuePair[4];
		nvps[0] =  new NameValuePair("secode",code);
		nvps[1] =  new NameValuePair("pageid","weibo");
		nvps[2] =  new NameValuePair("type","sass");
		nvps[3] =  new NameValuePair("_t","0");
		post.setRequestBody(nvps);
		int executeMethod = client.executeMethod(post);
    	System.out.println(post.getResponseBodyAsString());
	}
	
	private static String downloadValidImage(String cookie) throws HttpException, IOException{
		String ValidCodeUrl = "http://s.weibo.com/ajax/pincode/pin?type=sass%s";
		String url = String.format(ValidCodeUrl, DateTimeUtils.getCurrentTime());
		GetMethod get = new GetMethod(url);
		get.addRequestHeader("Cookie", cookie);
		client.executeMethod(get);
		OutputStream os = new FileOutputStream("code.jpg");
		byte[] b = get.getResponseBody();
		os.write(b, 0, b.length);
		os.close();
		Map<String,String> cookieMap = new HashMap<String,String>();
		setCookieFromMethod(cookieMap,get);
		return cookie.concat(getCookieStrFromMap(cookieMap));
	}
	
	private static String getCookieStrFromMap(Map<String,String> cookieMap){
		StringBuffer sb = new StringBuffer();
		for(Entry<String,String> e:cookieMap.entrySet()){
			sb.append(e.getKey()).append("=").append(e.getValue()).append(";");
		}
		return sb.toString();
	}
	
	private static void setCookieFromMethod(Map<String,String> cookieMap,HttpMethodBase method){
		Header[] headers = method.getResponseHeaders("Set-Cookie");
    	for (Header header : headers) {
			String cookie = header.toString().replace("Set-Cookie:", "").trim();
			if(cookie.toUpperCase().contains("DELETED")){
				break;
			}
			cookie = cookie.split(";",2)[0];
			String[] split = cookie.split("=", 2);
			String cookieKey = split[0];
			String cookieVal = split[1];
			cookieMap.put(cookieKey, cookieVal);
		}
	}
    
	private static String inputValidCode(){
		int inputTimes = 0;
		while(inputTimes++ < 3){
			try {
				System.out.println("Please input valid code ...");
				Thread.sleep(3 * 1000);
				BufferedReader br = new BufferedReader(new FileReader("code.txt"));
				String responseData = br.readLine();
				System.out.println("input valid num:" + responseData);
				br.close();
				// in.close();
				if (!StringUtils.isEmpty(responseData)) {
					return responseData;
				}
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		return "";
	}
}
