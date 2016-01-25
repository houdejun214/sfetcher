package com.sdata.core.parser.select;

import com.lakeside.core.utils.Assert;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataSelectorPipleBuilderTest {
	
	private static Document doc;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		if(doc==null){
            CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
            client.start();
            HttpGet get = new HttpGet("http://www.amazon.com/gp/product/B00186YU4M/ref=s9_simh_gw_g241_i4_r?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=desktop-3&pf_rd_r=0ZV3X5C8H6V3EZR25E81&pf_rd_t=36701&pf_rd_p=2084660942&pf_rd_i=desktop");
            HttpResponse response = client.execute(get, null).get();
            String content = EntityUtils.toString(response.getEntity());
            doc = Jsoup.parse(content);
		}
	}

	@Test
	public void test() {
		DataSelector selector = DataSelectorPipleBuilder.build("#productTitle");
		Object select = selector.select(doc);
		System.out.println("test:"+select);
	}

	@Test
	public void testTxt() {
		DataSelector selector = DataSelectorPipleBuilder.build("#productTitle|txt");
		Object select = selector.select(doc);
		System.out.println("testTxt:"+select);
	}

	@Test
	public void testAttr() {
		DataSelector selector = DataSelectorPipleBuilder.build("#productTitle|[class]");
		Object select = selector.select(doc);
		System.out.println("testAttr:"+select);
	}

	@Test
	public void testLink() {
		DataSelector selector = DataSelectorPipleBuilder.build("#fast-track-message .a-section a |link");
		Object select = selector.select(doc);
		System.out.println("testLink:"+select);
	}

	@Test
	public void testRegx() {
		DataSelector selector = DataSelectorPipleBuilder.build(".block-inner .first a |link|~/shop/(\\d+)/([a-zA-Z0-9_]+)/|[1]");
		Object select = selector.select(doc);
		System.out.println("testRegx:"+select);
	}

	@Test
	public void testFilter(){
		DataSelector selector = DataSelectorPipleBuilder.build(".desc-list dl|:[dt=服务]|dd em|txt");
		Object select = selector.select(doc);
		System.out.println("testFilter:"+select);
	}

	@Test
	public void testFormat(){
		DataSelector selector = DataSelectorPipleBuilder.build(".desc-list dl|:[dt=服务]|dd em|txt");
		Object select = selector.select(doc);
		System.out.println("testFilter:"+select);
	}

//	@Test
//	public void testSblingJsoup(){
//		DataSelector selector = DataSelectorPipleBuilder.build(".shop-detail-info .block-title|+  dt");
//		Object select = selector.select(doc);
//		System.out.println(select);
//		selector = DataSelectorPipleBuilder.build(".shop-detail-info .block-title|- dt");
//		select = selector.select(doc);
//		Assert.isNull(select);
//	}
}
