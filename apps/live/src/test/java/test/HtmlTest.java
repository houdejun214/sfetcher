package test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;


public class HtmlTest {
	
	@Test
	public void test() {
		
		String cookie ="pgv_pvi=5560400896; RK=W3omoVrbsW; suid=1816840818; pgv_si=s6218437632; verifysession=h02wwzgQqanuqrBfi-uKQLXDsgQMm6t-EmwOWgJ6_64xgwgj24kr24DHuua1apeLP29i5SkeuCBF3dP-TQaKlZbNg**; mb_reg_from=21; wbilang_1929981936=zh_CN; wbilang_10000=zh_CN; wb_regf=%3B0%3B%3Bmessage.t.qq.com%3B0; ptui_loginuin=1305327854; ptisp=; ptcz=f4b76ba392b6a24208c3d5f80e50eab6d049eb66b9bd3920e9ce937cf48e9552; luin=o1305327854; lskey=00010000c050a182e598e643ea89f805d1258ecf37e95c43ab4a0ae1933fe9bd27b5eeedc03d282ef34503b1; pt2gguin=o1305327854; uin=o1305327854; skey=@s8kQ0SCgN; p_uin=o1305327854; p_skey=CQzq3E3PQkVYxgb*U1lPAM2MxaBEVpLCF3RduJM*sQg_; pt4_token=r1BEg*I7BliYPUNk8Gw6hw__; p_luin=o1305327854; p_lskey=00040000435692b90dca31bb346d8adb19f73c045e2202e983fb9125dc933d9f679c100f3a321d45a2f8a70a; wbilang_1305327854=zh_CN; ts_last=t.qq.com/nusnext7458; ts_refer=ui.ptlogin2.qq.com/cgi-bin/login%3Fappid%3D46000101%26style%3D13%26lang%3D%26low_login%3D1%26hide_title_bar%3D1%26hide_close_icon%3D1%26self_regurl%3Dhttp%253; ts_uid=2876360151; pgv_info=ssid=s4832947173; pgv_pvid=8631385551; o_cookie=1305327854";
//		Tencent.getCookie("1305327854","nusnext");
//				Tencent.getCookie("1305327854","nusnext");
		String url = "http://search.t.qq.com/index.php?k=%E5%A8%83%E5%93%88%E5%93%88&s_time=20131212102921%2C20131212112921&s_advanced=1&s_m_type=1&p=0";
//		"http://search.t.qq.com/index.php?k=test&s_time=20131212%2C20131213&s_advanced=1&s_m_type=1&p=0"
		
		Map<String,String> header = new HashMap<String,String>();
		header.put("Cookie", cookie);
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
