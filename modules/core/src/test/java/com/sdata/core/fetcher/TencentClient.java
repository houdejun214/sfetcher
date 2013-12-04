package com.sdata.core.fetcher;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;

import com.sdata.util.MD5Security;

public class TencentClient {
	
	/**
	 * @param args
	*/
	@Test
	public void test() {

		HttpClient client = new DefaultHttpClient();

		client.getParams().setParameter(
				HttpConnectionParams.CONNECTION_TIMEOUT, 5000);
		try {
			
            try {
            		String uin="285900725";
            		String p="doujiang520";
            	   /********************* 获取验证码 ***********************/
            	   HttpGet get = new HttpGet("http://check.ptlogin2.qq.com/check?uin="
            	     + uin + "&appid=46000101&ptlang=2052&r=" + Math.random());
            	   get.setHeader("Host", "check.ptlogin2.qq.com");
            	   get.setHeader("Referer", "http://t.qq.com/?from=11");
            	   get.setHeader("User-Agent","Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0");
            	   HttpResponse response = client.execute(get);

            	   String entity = EntityUtils.toString(response.getEntity());
            	   String[] checkNum = entity.substring(entity.indexOf("(") + 1,entity.lastIndexOf(")")).replace("'", "").split(",");
            	   System.out.println(checkNum[0]);
            	   System.out.println(checkNum[1]);
            	   System.out.println(checkNum[2].trim());
            	   System.out.println(checkNum[2].trim().replace("\\x", ""));
            	   String pass = "";

            	   /******************** *加密密码 ***************************/
            	   pass = MD5Security.GetPassword(checkNum[2].trim(),p,checkNum[1].trim());
            	   
            	   /************************* 登录 ****************************/
            	   get = new HttpGet(
            	     "http://ptlogin2.qq.com/login?ptlang=2052&u="+ uin + "&p=" + pass+ "&verifycode="+ checkNum[1]
            	       + "&aid=46000101&u1=http%3A%2F%2Ft.qq.com&ptredirect=1&h=1&from_ui=1&dumy=&fp=loginerroralert&action=4-12-14683&g=1&t=1&dummy=");
            	   get.setHeader("Connection", "keep-alive");
            	   get.setHeader("Host", "ptlogin2.qq.com");
            	   get.setHeader("Referer", "http://t.qq.com/?from=11");
            	   get.setHeader("User-Agent","Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0");
            	   response = client.execute(get);
            	   entity = EntityUtils.toString(response.getEntity());
            	   System.out.println(entity);
            	   if (entity.indexOf("登录成功") > -1) {
	            	    get = new HttpGet("http://t.qq.com");
	            	    response = client.execute(get);
	            	    entity = EntityUtils.toString(response.getEntity());
	            	    Document doc = Jsoup.parse(entity);
	            	    Element es = doc.getElementById("topNav1");
	            	    String displayName = "";
	            	    if (es != null) {
	            	     Elements e = es.getElementsByTag("u");
	            	     if (e != null && e.size() > 0)
	            	      displayName = e.get(0).text();
	            	    }
            	   }
            	   get = new HttpGet("http://zhaoren.t.qq.com/affectRank.php?id=0&timegap=day");
            	   response = client.execute(get);
            	   entity = EntityUtils.toString(response.getEntity());
            	   System.out.println(entity);
            	   System.out.println("登陆成功");

            	  } catch (Exception e) {
	            	   e.printStackTrace();
	            	   System.out.println(e.getMessage());
            	  }
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

	}

}