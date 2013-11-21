package com.sdata.component.parser;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.JsoupUtils;
import com.sdata.core.util.WebPageDownloader;

public class FantongParser extends SdataParser{
	
	public static final String dpHost="http://www.fantong.com";
	public static final String commentsUrl ="http://www.fantong.com/?s=biz&a=comments_list&bid=%s&type=0&cate=0&pid=0&gp=0&page=%s";
	
	public static final Log log = LogFactory.getLog("SdataCrawler.FantongParser");
	private  String topArea;
	private  String secondArea;
	private int maxDatum;
	private static Pattern shopPattern = Pattern.compile("^http://www\\.fantong\\.com/shop/(.*)$");
	
	public FantongParser(Configuration conf,RunState state){
		setConf(conf);
		this.state = state;
		topArea = this.getConf(Constants.SITE_TOP_AREA);
		secondArea = this.getConf(Constants.SITE_SECOND_AREA);
		maxDatum = this.getConfInt(Constants.SITE_MAX_DATUM, 500);
	}

	/**
	 * @param content
	 * @return
	 */
	public List<Map<String,Object>> parseTopCategoryList(String content){
		Document doc = parseHtmlDocument(content);
		if(doc == null){
			return null;
		}
		JSONArray categories = new JSONArray();
		Elements els = doc.select(topArea);
		if( els==null || els.size()==0){
			return null;
		}
		Iterator<Element> iterator = els.iterator();
		while(iterator.hasNext()){
			Element el = iterator.next();
			Elements cates = el.select("[href]");
			Iterator<Element> cateItor = cates.iterator();
			while(cateItor.hasNext()){
				Element cate = cateItor.next();
				if(cate==null){
					continue;
				}
				String text = cate.text();
				String link = cate.attr("href");
				JSONObject json = new JSONObject();
				json.put(Constants.QUEUE_KEY, link);
				json.put(Constants.QUEUE_NAME, text);
				json.put(Constants.QUEUE_URL, link);
				json.put(Constants.QUEUE_DEPTH, Constants.QUEUE_DEPTH_ROOT);
				json.put(Constants.QUEUE_DISPOSE,Constants.FLAG_NO);
				categories.add(json);
			}
		}
		return categories;
	}
	
	@Override
	public ParseResult parseList(RawContent c) {
		
		int depth = toInt(c.getMetadata(Constants.QUEUE_DEPTH),1);
		FantongParseResult result = new FantongParseResult();
		Document doc = parseHtmlDocument(c);
		if(doc == null){
			log.warn("fetch content empty："+c.getUrl());
			return null;
		}
		Element span = doc.select(".cutitem").first();
		if(span != null){
			int count = getCount(span.text());
			if(count < maxDatum){
				Map map = new HashMap();
				addDatumList(c.getUrl(), result,map);
				return result;
			}
		}
	
		Elements chanels = doc.select(secondArea);
		if(chanels==null||chanels.size() ==0){
			log.warn("get secode chanel empty："+c.getUrl());
			return null;
		}
		Iterator<Element> iterator = chanels.iterator();
		List<Map<String,Object>> newCategoryList = new ArrayList<Map<String,Object>>();
		while(iterator.hasNext()){
			Element el = iterator.next();
			String url = getUrl(el.attr("href"));
			Map<String,Object> cate = new HashMap<String,Object>();
			cate.put(Constants.QUEUE_KEY, url);
			cate.put(Constants.QUEUE_NAME, el.text());
			cate.put(Constants.QUEUE_URL,url);
			cate.put(Constants.QUEUE_DEPTH, depth+1);
			cate.put(Constants.QUEUE_DISPOSE, Constants.FLAG_NO);
			newCategoryList.add(cate);
		}
		result.setNewCategoryList(newCategoryList);
		return result;
	}

	/**
	 * @param url
	 * @param result
	 */
	public void addDatumList(String url,ParseResult result,Map map) {
		String content = download(url);
		Document doc = parseHtmlDocument(content);
		if(doc==null){
			return;
		}
		Elements els = doc.select(".result_item .content h2 a:not(.fd)");
		if(els==null||els.size() == 0) {
			log.warn("get shop list empty:"+url);
			return;
		}
		Iterator<Element> iterator = els.iterator();
		while(iterator.hasNext()){
			Element e = iterator.next();
			String shop = getUrl(e.attr("href"));
			String shopId = getShopId(shop);
			if(StringUtils.isEmpty(shopId)||this.isExist(shopId,map)){
				continue;
			}
			map.put(shopId, 1);
			Map<String,Object> meta = new HashMap<String,Object>();
			FetchDatum datum = new FetchDatum();
			meta.put(Constants.QUEUE_NAME, e.text());
			meta.put(Constants.QUEUE_URL,shop);
			meta.put(Constants.SHOP_ID,shopId);
			datum.setId(shopId);
			datum.setUrl(shop);
			datum.setMetadata(meta);
			result.addFetchDatum(datum);
		}
		//go on next page
		Element next = doc.select("div.pagination :contains(下一页)").first();
		if(next!=null&&!StringUtils.isEmpty((next.attr("href")))){
			try {
				addDatumList(URIUtil.encodeQuery(getUrl(next.attr("href").substring(1))), result,map);
			} catch (URIException e) {
				log.error("next page error"+e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		Document doc = parseHtmlDocument(content);
		if(StringUtils.isEmpty(c.getContent())){
			throw new NegligibleException("web content is empty!");
		} 
		String url = c.getUrl();
		JSONObject matadata= new JSONObject();
		// get the matadata information from the content
		String shopId = this.getShopId(url);
		matadata.put("shopurl",url);
		matadata.put(Constants.SHOP_NAME,JsoupUtils.getText(doc, ".res_introduction h1"));
		matadata.put("address",JsoupUtils.getText(doc, ".res_introduction dd:contains(地址)"));
		matadata.put("telephone",JsoupUtils.getText(doc, ".res_introduction dd:contains(电话)"));
		matadata.put("score",JsoupUtils.getText(doc, ".res_introduction i"));
		String geo = getGeo(doc.select("body").first().attr("onload"));
		if(!StringUtils.isEmpty(geo))
			matadata.put("geo",geo);
		
		String text = JsoupUtils.getText(doc, ".res_introduction dd:contains(标签)");
		if(!StringUtils.isEmpty(text)){
			matadata.put("mark",text);
		}

		text = JsoupUtils.getText(doc,".res_introduction dd span");
		if(!StringUtils.isEmpty(text)){
			matadata.put("price",text);
		}
		text = JsoupUtils.getText(doc,".but_yu_r em");
		if(!StringUtils.isEmpty(text)){
			matadata.put("discount",text);
		}
		// get some basic information
		text = JsoupUtils.getText(doc,".hotel_reservation dd:contains(电话)");
		if(!StringUtils.isEmpty(text)){
			matadata.put("telephone", text);
		}
		text = JsoupUtils.getListText(doc,".hotel_reservation a.first");
		if(!StringUtils.isEmpty(text)){
			matadata.put("purpose", text);
		}
		text =  JsoupUtils.getText(doc,".hotel_reservation dd:contains(营业时间)");
		if(!StringUtils.isEmpty(text)){
			matadata.put("open_time", text);
		}
		text =  JsoupUtils.getText(doc,".hotel_reservation dd:contains(注意事项)");
		if(!StringUtils.isEmpty(text)){
			matadata.put("notice", text);
		}

		// get some control information
		List<Element> list = JsoupUtils.getList(doc, ".kw_hj em");
		if(list!=null&&list.size() > 0){
			//Element el = (Element)CollectionUtils.get(list, 0);
			matadata.put("taste", JsoupUtils.getListItemText(list,0,"em"));
			//el = (Element)CollectionUtils.get(list, 1);
			matadata.put("surroundings", JsoupUtils.getListItemText(list,1,"em"));
			//el = (Element)CollectionUtils.get(list, 2);
			matadata.put("service", JsoupUtils.getListItemText(list,2,"em"));
		}
		
		String commentsLink = getUrl(JsoupUtils.getLink(doc, ".res_introduction a:contains(评)"));
		matadata.put("comments_url", commentsLink);
		String picLink = getUrl(JsoupUtils.getLink(doc, ".res_introduction a:contains(查看全部)"));
		matadata.put("pic_url", picLink);
		
		matadata.put("descShort", JsoupUtils.getText(doc, ".txt-short"));
		matadata.put("descLong", JsoupUtils.getText(doc, ".txt-long"));
		JSONArray comments = this.getComments(shopId,commentsLink);
		matadata.put(Constants.REVIEWS, comments);
		matadata.put("images", this.getPics(picLink));
		matadata.put("siteType", "fantong");
		matadata.put(Constants.SHOP_ID, shopId);
		matadata.put(Constants.OBJECT_ID, shopId);
		matadata.put(Constants.CYCLE, state.getCycle());
		result.setMetadata(matadata);
		return result;
	}
		
	private String getShopId(String shop){
		int b = shop.indexOf("-");
		return shop.substring(b+1,shop.length()-1);
//		String result = StringUtils.hash(shop);
//		int l = Integer.parseInt(result);
//		if(l>0) {
//			result = "1".concat(result);
//		}else{
//			result = "2".concat(String.valueOf(Math.abs(l)));
//		}
//		return result;
	}
	
	private String getUrl(String url){
		if(url==null){
			return "";
		}
		url = dpHost + url;
		
		return url;
	}
	

	private boolean isExist(String shopId,Map map){
		if(shopId==null){
			return false;
		}
		if(map.containsKey(shopId)){
			return true;
		}
		return false;
	}
	
	private Integer getCount(String text){
		int result = 0;
		if(text == null){
			return result;
		}
		int b = text.indexOf("（");
		int e = text.indexOf("）");
		if(b<0||e<0){
			return result;
		}
		result = Integer.parseInt(text.substring(b+1,e));
		return result;
	}
	
	public String download(String url){
		String content = null;
		while(true){
			//sleep(10);
			//log.info(url);
			content = WebPageDownloader.download(url);
			if(StringUtils.isEmpty(content)){
				log.info("*********获取内容为空,正在等待,5分钟后继续访问...");
				sleep(5*60);
				continue;
			}
			if(content.contains("对不起，你访问的太快了")){
				log.info("*********访问频率太快，正在等待，10分钟后继续访问...");
				sleep(60*10);
				continue;
			}
			return content;
		}
	}

	private void sleep(int s){
		try {
			Thread.sleep(s*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private JSONArray getComments(String shopId,String commentsLink) {
		JSONArray comments = new JSONArray();
		while(!StringUtils.isEmpty(commentsLink)){
			String download = this.download(commentsLink);
			Document doc = this.parseHtmlDocument(download);
			List<Element> recommend_itor = JsoupUtils.getList(doc,".comment-main");
			for(int i=0;i<recommend_itor.size();i++){
				JSONObject comment= new JSONObject();
				Element recommend_el = recommend_itor.get(i);
				comment.put("author", JsoupUtils.getText(recommend_el, ".score-show strong"));
				comment.put("author_url", getUrl(JsoupUtils.getLink(recommend_el, ".score-show a")));
				comment.put("time",JsoupUtils.getText(recommend_el, ".pubtime"));
				comment.put("score",JsoupUtils.getList(recommend_el,".clearfix [src$=starnew_small_on.gif]").size());
				List<Element> scoreItem = JsoupUtils.getList(recommend_el, "ul.clearfix li strong");
				comment.put("service",JsoupUtils.getListItemText(scoreItem, 0, "strong"));
				comment.put("taste",JsoupUtils.getListItemText(scoreItem, 1, "strong"));
				comment.put("surroundings",JsoupUtils.getListItemText(scoreItem, 2, "strong"));
				comment.put("comment",JsoupUtils.getText(recommend_el, ".txt"));
				comment.put("sid",shopId);
				comment.put(Constants.OBJECT_ID,shopId+"_"+(i+1));
				comments.add(comment);
			}
			commentsLink = getCommentsNextPage(doc);
		}
		return comments;
	}
	
	private String getCommentsNextPage(Document doc){
		Element select = doc.select(".pagination a:contains(下一页)").first();
		if(select == null){
			return null;
		}
		String func = select.attr("onclick");
		if(StringUtils.isEmpty(func)){
			return null;
		}
		int sb = func.indexOf("(");
		int se = func.indexOf(",");
		int pb = func.lastIndexOf(",");
		int pe = func.indexOf(")");
		if(sb==-1||se==-1||pb==-1||pe==-1){
			return null;
		}
		String shopid = func.substring(sb+1, se);
		String page = func.substring(pb+1,pe);
		return String.format(commentsUrl, shopid,page);
	}
	

	
	private JSONArray getPics(String picLink) {
		JSONArray pics = new JSONArray();
		while(!StringUtils.isEmpty(picLink)){
			String download = this.download(picLink);
			Document doc = this.parseHtmlDocument(download);
			List<Element> pic_itor = JsoupUtils.getList(doc,"#allPicList .abc");
			for(int i=0;i<pic_itor.size();i++){
				Element pic_el = pic_itor.get(i);
				String img  = changeImageSize(JsoupUtils.getLink(pic_el,"img"));
				if(!StringUtils.isEmpty(img)){
					pics.add(img);
				}
			}
			picLink = getPicNextPage(doc);
		}
		return pics;
	}
	

	private String getPicNextPage(Document doc){
		String link = JsoupUtils.getLink(doc,".pagination a:contains(下一页)");
		if(StringUtils.isEmpty(link)){
			return null;
		}
		return this.getUrl(link);
	}
	private String changeImageSize(String link){
		if(StringUtils.isEmpty(link)){
			return null;
		}
		return link.replace("middle", "origin");
	}
	
	private String getGeo(String content){
		if(StringUtils.isEmpty(content)){
			return null;
		}
		int b = content.lastIndexOf("(");
		int e = content.lastIndexOf(")");
		if(b==-1||e==-1){
			return null;
		}
		String result = content.substring(b+1,e).replaceAll("'", "");
		String[] split = result.split(",");
		if(split.length!=2){
			return null;
		}
		return split[1].concat(",").concat(split[0]);
	}
}
