package test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.lakeside.download.http.HttpPageLoader;
import com.tencent.weibo.utils.Tencent;


public class HtmlTest {
	
	@Test
	public void test() throws Exception {
		
//		String cookie ="mb_reg_from=8; RK=W0CmIUS7Ma; wbilang_2091496801=zh_CN; wbilang_10000=zh_CN; verifysession=h02FhKziizktWe6iClZMx5Bsb6nfDYtM_Hsp2V9ntJkJimtCdK8tFZNaz8wxXdLmboiKjfUiSsbbkVtyVqrgFCGUg**; ptui_loginuin=2091496801; ptisp=os; ptcz=32dc3e2e6360536b0c009d6f9e810941969401be6653b8556bc959cab318205b; pt2gguin=o2091496801; uin=o2091496801; skey=@bWHrL6xjW; p_uin=o2091496801; p_skey=S5RZyAiq*nnlnFQxO0NQuI1leb2OPsXQmHpGVH0YK9Q_; pt4_token=S77envOeyzbp34q8buTAqg__; pgv_info=ssid=s390766216; ts_refer=ui.ptlogin2.qq.com/cgi-bin/login%3Fappid%3D46000101%26style%3D13%26lang%3D%26low_login%3D1%26hide_title_bar%3D1%26hide_close_icon%3D1%26self_regurl%3Dhttp%253; pgv_pvid=1345006017; o_cookie=2091496801; ts_uid=4556937966; wb_regf=%3B0%3B%3Bmessage.t.qq.com%3B0";
		String cookie = Tencent.getCookie("2091496801","lmsnext");
		System.out.print(cookie);
//		String redirectToWeibo = Tencent.redirectToWeibo();
//		System.out.println(redirectToWeibo);
//				Tencent.getCookie("1305327854","nusnext");
		String url = "http://search.t.qq.com/index.php?k=%E5%A8%83%E5%93%88%E5%93%88&s_time=20131212102921%2C20131212112921&s_advanced=1&s_m_type=1&p=0";
//		"http://search.t.qq.com/index.php?k=test&s_time=20131212%2C20131213&s_advanced=1&s_m_type=1&p=0"
		
		Map<String,String> header = new HashMap<String,String>();
		header.put("Cookie", cookie);
		String content = HttpPageLoader.getDefaultPageLoader().download(header, url).getContentHtml();
		System.out.println(content);
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
