package test;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.junit.Test;

import com.lakeside.core.utils.time.DateTimeUtils;


public class HtmlTest {
	
	@Test
	public void test() throws Exception {
		HttpClient client = new HttpClient();
		String code = "2a2u";
//		1389841009765
		String cookie = "_s_tentry=blog.ifeng.com; UOR=blog.ifeng.com,widget.weibo.com,blog.ifeng.com; login_sid_t=cb645b12a1b4266c652f44196d0c0416; Apache=3795269723050.2964.1389839818773; SINAGLOBAL=3795269723050.2964.1389839818773; ULV=1389839818788:1:1:1:3795269723050.2964.1389839818773:; SUE=es%3Dc0aac6563568bda8e3bdaaec56813bf8%26ev%3Dv1%26es2%3D4d743a262e0ede85cb07065cad0c6e26%26rs0%3DE4ZXzG22jsx1lsFeP0ceEcLDkQQN6h5tp3YiVR2g6D47gIlNwldFfDS4m7dhqd0wZTAlzNXuJRDiwBMCcp%252B7NaTIjI42nWrN3Dz9CEWmJUKUcG7dstXbApP6cUxC3hc%252BogWdElhdwkBOa115KTIgy3pVO9Kq%252FKmFtjbAb6raA28%253D%26rv%3D0; SUP=cv%3D1%26bt%3D1389839826%26et%3D1389926226%26d%3Dc909%26i%3D2979%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D0%26st%3D0%26uid%3D3956779699%26name%3D2033701349%2540qq.com%26nick%3Dsense-04%26fmp%3D%26lcp%3D; SUS=SID-3956779699-1389839826-XD-or8bc-c4f69330b9220dee7d70174af931480c; ALF=1392431826; SSOLoginState=1389839826; wvr=5; SWB=usrmd1367; ULOGIN_IMG=13898419850091; WBStore=41d2eb2fbba1c786|undefined";
		String url = "http://s.weibo.com/ajax/pincode/verified?__rnd="+DateTimeUtils.getCurrentTime();
//		url +="&pageid=weibo&type=sass&_t=0&secode="+code;
//		Map<String,String> map = new HashMap<String,String>();
//		map.put("Cookie", cookie);
//		map.put("X-Requested-With", "XMLHttpRequest");
//		map.put("Origin", "http://s.weibo.com");
//		map.put("Referer", "http://s.weibo.com/weibo/%25E8%2583%25A5%25E6%25B8%25A1%25E5%2590%25A7?topnav=1&wvr=5&Refer=top_hot");
//		map.put("Host", "s.weibo.com");
//		map.put("Content-Type", "application/x-www-form-urlencoded");
//		HttpPage download = HttpPageLoader.getDefaultPageLoader().download(map,url);
//		System.out.println(download.getContentHtml());
		
//		map.put("Referer", "http://s.weibo.com/weibo/%25E8%2583%25A5%25E6%25B8%25A1%25E5%2590%25A7?topnav=1&wvr=5&Refer=top_hot");
		PostMethod post = new PostMethod(url);
//		post.setRequestHeader("Accept", "*/*");
		post.setRequestHeader("Accept-Language","en");
		post.setRequestHeader("Accept-Encoding","gzip,deflate,sdch");
		post.setRequestHeader("Cookie", cookie);
		post.setRequestHeader("X-Requested-With", "XMLHttpRequest");
//		post.setRequestHeader("Origin", "http://s.weibo.com");
//		post.setRequestHeader("Content-length", "39");
//		post.setRequestHeader("Host", "s.weibo.com");
		post.setRequestHeader("User-Agent","Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.72 Safari/537.36");
		post.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
		post.setRequestHeader("Referer", "http://s.weibo.com/weibo/x&Refer=STopic_box");
		post.addParameter("secode", code);
		post.addParameter("pageid", "weibo");
		post.addParameter("type", "sass");
		post.addParameter("_t", "0");
		int executeMethod = client.executeMethod(post);
    	System.out.println(post.getResponseBodyAsString());
		
//		Date date = new Date();
//		String format = DateTimeUtils.format(date, "yyyy-MM-dd-H");
//		System.out.println(format);
		
//	    	String cookie = Weibo.getCookie("2033701349@qq.com","lmsnext");
//	    	System.out.println(cookie);
//	    	Map<String,String> header = new HashMap<String,String>();
//	    	StringBuffer sb = new StringBuffer();
//	    	sb.append("tgc=TGT-Mzk1Njc3OTY5OQ==-1389348694-hk-8926E8C033BD077321168010D35FDC1E;ALC=ac%3D2%26bt%3D1389348694%26cv%3D4.0%26et%3D1391940694%26uid%3D3956779699%26vf%3D0%26vt%3D0%26es%3D4fe458489390e60027ccbec60069423;");
//	    	sb.append("LT=1389348694;sso_info=v02m6alo5qztbOZlrmzmZK0sI2CmbWalpC9jLOktY2jnLeOk5i5jpDAwA==;wvr=5;un=2033701349@qq.com;SUE=es%3D8c3bf51d554089043f825125d5c8ad32%26ev%3Dv1%26es2%3D03c193475ad0fe3ee7d67b7eb8497895%26rs0%3DuV5waa7X7EKWPdeiX8OHbSzMTuPxp0RTZmYQbmBn9ElsFIcxuvFA9GeFYYhznR%252FM3zVNidCVJCLQ9J5r8JQmcW%252Bt3gJV41tXB6c0G7ogrzgdn8G%252F%252FscYBwOyaRtlRlsJtoPfX4TxmLHedSeViwVtg0AGOgtHV7OLtzE7MN0LP7E%253D%26rv%3D0;SUP=cv%3D1%26bt%3D1389348696%26et%3D1389435096%26d%3Dc909%26i%3D790d%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D2%26st%3D0%26uid%3D3956779699%26name%3D2033701349%2540qq.com%26nick%3Dsense-04%26fmp%3D%26lcp%3D;SUS=SID-3956779699-1389348696-XD-vp3c2-49f61d1ceb926137042e38baf9157ee0;ALF=1391940694;SSOLoginState=1389348696;UUG=usrmdins41447;U_TRS1=0000000a.96b67333.52cfc765.ff92354a;U_TRS2=0000000a.96c67333.52cfc765.5ea2f7fd;login_sid_t=1fe41e1f7a9ded44751220484d991cb5;inuid=3956779699;inuid=3956779699;v5reg=usr1459;");
//	    	header.put("Cookie", cookie);
//	    	HttpPage download = HttpPageLoader.getDefaultPageLoader().download("http://www.163.com/");
		
//		String cookie ="mb_reg_from=8; RK=W0CmIUS7Ma; wbilang_2091496801=zh_CN; wbilang_10000=zh_CN; verifysession=h02FhKziizktWe6iClZMx5Bsb6nfDYtM_Hsp2V9ntJkJimtCdK8tFZNaz8wxXdLmboiKjfUiSsbbkVtyVqrgFCGUg**; ptui_loginuin=2091496801; ptisp=os; ptcz=32dc3e2e6360536b0c009d6f9e810941969401be6653b8556bc959cab318205b; pt2gguin=o2091496801; uin=o2091496801; skey=@bWHrL6xjW; p_uin=o2091496801; p_skey=S5RZyAiq*nnlnFQxO0NQuI1leb2OPsXQmHpGVH0YK9Q_; pt4_token=S77envOeyzbp34q8buTAqg__; pgv_info=ssid=s390766216; ts_refer=ui.ptlogin2.qq.com/cgi-bin/login%3Fappid%3D46000101%26style%3D13%26lang%3D%26low_login%3D1%26hide_title_bar%3D1%26hide_close_icon%3D1%26self_regurl%3Dhttp%253; pgv_pvid=1345006017; o_cookie=2091496801; ts_uid=4556937966; wb_regf=%3B0%3B%3Bmessage.t.qq.com%3B0";
//		String cookie = Tencent.getCookie("2091496801","lmsnext");
//		System.out.print(cookie);
//		String redirectToWeibo = Tencent.redirectToWeibo();
//		System.out.println(redirectToWeibo);
//				Tencent.getCookie("1305327854","nusnext");
//		String url = "http://search.t.qq.com/index.php?k=%E5%A8%83%E5%93%88%E5%93%88&s_time=20131212102921%2C20131212112921&s_advanced=1&s_m_type=1&p=0";
//		"http://search.t.qq.com/index.php?k=test&s_time=20131212%2C20131213&s_advanced=1&s_m_type=1&p=0"
//		
//		Map<String,String> header = new HashMap<String,String>();
//		header.put("Cookie", cookie);
//		String content = HttpPageLoader.getDefaultPageLoader().download(header, url).getContentHtml();
//		System.out.println(content);
//	    final WebClient webClient = new WebClient();
//	    final HtmlPage page = webClient.getPage("http://weibo.com/1496850204/zqLUsvKEn");
//	    System.out.println(page);
//	    final String pageAsXml = page.asXml();
//	    HtmlElement body = page.getBody();
//	    System.out.println(body);
//	    System.out.println(pageAsXml);
//	    final String pageAsText = page.asText();
//	    System.out.println(pageAsText);
//
//	    webClient.closeAllWindows();
//		SdataHtmlPaserContext context= new SdataHtmlPaserContext();
//		context.put("name", "qmm");
//		context.put("title", "professor");
		
//		String res = formattor.format(Arrays.asList("string1","string2"), context);
//		System.out.println(format);
//		System.out.println(res);
	}

}
