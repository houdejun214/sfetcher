package com.sdata.core.parser.select;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.util.Assert;

import com.sdata.core.http.HttpPageLoader;

public class DataSelectorPipleBuilderTest {
	
//	private static Document doc;
//
//	@BeforeClass
//	public static void setUpBeforeClass() throws Exception {
//		if(doc==null){
//			String content = HttpPageLoader.getDefaultPageLoader().download("http://www.dianping.com/shop/5422956?KID=114027").getContentHtml();
//			doc = Jsoup.parse(content);
//		}
//	}
//
//	@Test
//	public void test() {
//		DataSelector selector = DataSelectorPipleBuilder.build(".shop-name .shop-title");
//		Object select = selector.select(doc);
//		System.out.println("test:"+select);
//	}
//
//	@Test
//	public void testTxt() {
//		DataSelector selector = DataSelectorPipleBuilder.build(".shop-name .shop-title|txt");
//		Object select = selector.select(doc);
//		System.out.println("testTxt:"+select);
//	}
//	
//	@Test
//	public void testAttr() {
//		DataSelector selector = DataSelectorPipleBuilder.build(".shop-name .shop-title|[itemprop]");
//		Object select = selector.select(doc);
//		System.out.println("testAttr:"+select);
//	}
//	
//	@Test
//	public void testLink() {
//		DataSelector selector = DataSelectorPipleBuilder.build(".block-inner .first a |link");
//		Object select = selector.select(doc);
//		System.out.println("testLink:"+select);
//	}
//	
//	@Test
//	public void testRegx() {
//		DataSelector selector = DataSelectorPipleBuilder.build(".block-inner .first a |link|~/shop/(\\d+)/([a-zA-Z0-9_]+)/|[1]");
//		Object select = selector.select(doc);
//		System.out.println("testRegx:"+select);
//	}
//	
//	@Test
//	public void testFilter(){
//		DataSelector selector = DataSelectorPipleBuilder.build(".desc-list dl|:[dt=服务]|dd em|txt");
//		Object select = selector.select(doc);
//		System.out.println("testFilter:"+select);
//	}
//	
//	@Test
//	public void testFormat(){
//		DataSelector selector = DataSelectorPipleBuilder.build(".desc-list dl|:[dt=服务]|dd em|txt");
//		Object select = selector.select(doc);
//		System.out.println("testFilter:"+select);
//	}
//	
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
