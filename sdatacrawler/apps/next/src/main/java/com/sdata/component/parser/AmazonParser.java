package com.sdata.component.parser;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.QueryUrl;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.lakeside.core.utils.UrlUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.HtmlUtils;
import com.sdata.core.util.WebPageDownloader;


public class AmazonParser extends SdataParser{
	
	public static final String AmazonHost="http://www.amazon.com";
	
	public static final Log log = LogFactory.getLog("SdataCrawler.AmazonParser");
	
	private static Pattern CatePattern1 = Pattern.compile("^/s/ref=(.*)$");
	private static Pattern CatePattern2 = Pattern.compile("^/(.*)/b/ref=topnav_(.*)$");

	private static Pattern ProductUrlPattern = Pattern.compile("^http://www\\.amazon\\.com/(.*)/dp/(.*)/ref=(.*)$");

	private static Pattern ImageUrlMartchPattern = Pattern.compile("addToIVImageSet\\('(.*?)', '(.*?)'\\);");
	
	public AmazonParser(Configuration conf,RunState state){
		setConf(conf);
		this.state = state;
	}

	@Override
	public ParseResult parseList(RawContent c) {
		//String parentUrl = c.getUrl();
		int depth = toInt(c.getMetadata(Constants.QUEUE_DEPTH),1);
		String curUrl = c.getUrl();
		AmazonParseResult result = new AmazonParseResult();
		Document doc = parseHtmlDocument(c);
		if(doc==null){
			return null;
		}
		
		String categorySelector = "#leftcol a,#leftNav a";
		
		// check whether it is a category product browse page which used to browse product list.
		Element bread = doc.select("#breadCrumbDiv #breadCrumb").first();
		if(bread!=null){
			String breadcrumb = bread.text();
			String category = breadcrumb.replaceAll("›", "/");
			Elements products = doc.select("#rightContainerATF #rightResultsATF div.results div.prod");
			Iterator<Element> iterator2 = products.iterator();
			while(iterator2.hasNext()){
				Element el = iterator2.next();
				Element title = el.select("h3 a").first();
				//not find 
				if(title==null) continue;
				String url = title.attr("href");
				String name = title.text();
				Map<String,Object> meta = new HashMap<String,Object>();
				FetchDatum datum = new FetchDatum();
				meta.put(Constants.QUEUE_NAME, name);
				meta.put(Constants.QUEUE_URL,getUrl(url));
				meta.put("category", category);
				String productId = getProductId(url);
				if(StringUtils.isEmpty(productId)){
					log.warn("product Id is empty! ");
					continue;
				}
				meta.put("productId", productId);
				datum.setId(productId);
				datum.setUrl(url);
				datum.setMetadata(meta);
				if(!isAmazonHostLink(meta)){
					continue;
				}
				result.addFetchDatum(datum);
			}
			
			Element nextPager = doc.select("#centerBelow #bottomBar #pagn .pagnNext #pagnNextLink").first();
			if(nextPager!=null){
				String nextUrl = nextPager.attr("href");
				if(!nextUrl.startsWith("/")){
					nextUrl = curUrl + nextUrl;
				}else{
					nextUrl = AmazonHost + nextUrl;
				}
				result.setNextUrl(nextUrl);
			}
			
			categorySelector+=",#center #bestRefinement a";
		}else {
			categorySelector+=",#navCatSubnav a, #centercol a";
		}
		
		// left column & centre column
		Elements anchors = doc.select(categorySelector);
		Iterator<Element> iterator = anchors.iterator();
		List<Map<String,Object>> newCategoryList = new ArrayList<Map<String,Object>>();
		while(iterator.hasNext()){
			Element ancher = iterator.next();
			if(ancher.select("img").first()!=null){
				continue;
			}
			String ancherHref = ancher.attr("href");
			boolean isCategoryUrl = isMatch(CatePattern1,ancherHref) ||  isMatch(CatePattern2,ancherHref);
			if(isCategoryUrl){
				Map<String,Object> cate = new HashMap<String,Object>();
				cate.put(Constants.QUEUE_KEY, ancher.text());
				cate.put(Constants.QUEUE_NAME, ancher.text());
				cate.put(Constants.QUEUE_URL,getUrl(ancherHref));
				cate.put(Constants.QUEUE_DEPTH, depth+1);
				cate.put(Constants.QUEUE_DISPOSE, Constants.FLAG_NO);
				if(!isAmazonHostLink(cate)){
					continue;
				}
				newCategoryList.add(cate);
			}
			
		}
		result.setNewCategoryList(newCategoryList);
		return result;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		Document doc = parseHtmlDocument(c);
		if(doc==null){
			return null;
		}
		String pid = String.valueOf(c.getMetadata("productId"));
		if(StringUtils.isEmpty(pid)){
			return null;
		}
		Map<String,Object> jobj= new HashMap<String,Object>();
		jobj.put("productName", this.selectText(doc, ".parseasinTitle #btAsinTitle"));
		jobj.put("productId",pid);
		//jobj.put(Constants.OBJECT_ID, pid);
		jobj.put("purl", c.getUrl());
		jobj.put("category", c.getMetadata("category"));
		jobj.put("isfetched", "1");
		jobj.put("reviewStar", this.selectText(doc, ".jumpBar .crAvgStars a span span"));
		jobj.put("listPrice", this.selectText(doc, "#listPriceValue"));
		jobj.put("price", this.selectText(doc,"#actualPriceValue .priceLarge"));
		jobj.put("images", this.parseImageList(doc));
		jobj.put("stocksSatus", this.selectText(doc, ".buying span"));
		jobj.put("technicalDetails", HtmlUtils.filterExcludeTags(this.selectSiblingBlockContentHtml(doc, "#technical_details", ".content"),"img"));
		jobj.put("productDetails", HtmlUtils.filterExcludeTags(this.selectSiblingBlockContentHtml(doc, ".bucket h2:contains(Product Details)", ".content"),"img"));
		jobj.put("description",  HtmlUtils.filterExcludeTags(this.selectHtml(doc, "#productDescription .content"),"img"));
		List<Object> parseReviews = this.parseReviews(pid,doc);
		if(parseReviews == null){
			log.warn("amazon product not find reviews,product:"+ c.getUrl());
		}
		jobj.put(Constants.REVIEWS, parseReviews);
		jobj.put("siteType", "amazon");
		jobj.put(Constants.CYCLE, state.getCycle());
		result.setMetadata(jobj);
		return result;
	}

	private JSONArray parseImageList(Document doc) {
		JSONArray images = new JSONArray();
		Element first = doc.select("#PIAltImagesDiv").first();
		if(first!=null){
			Elements select = first.select("#thumb_strip .productThumbnail img");
			Map<String,String> maps = new HashMap<String,String>();
			Elements siblingElements = first.siblingElements();
			String content = siblingElements.html();
			Matcher matcher = ImageUrlMartchPattern.matcher(content);
			while(matcher.find()){
				String url1 = getImageUrl(matcher.group(1));
				String url2 =getImageUrl(matcher.group(2));
				maps.put(url1, url2);
			}
			Iterator<Element> iterator = select.iterator();
			while(iterator.hasNext()){
				Element el = iterator.next();
				String urlThumb = el.attr("src");
				String imageUrl = getImageUrl(urlThumb);
				if(maps.containsKey(imageUrl)){
					imageUrl = maps.get(imageUrl);
				}
				images.add(imageUrl);
			}
		}else{
			// only one image
			Element imageCell = doc.select("#prodImageCell img").first();
			if(imageCell!=null){
				String src = imageCell.attr("src");
				String imageUrl = getImageUrl(src);
				images.add(imageUrl);
			}
		}
		return images;
	}

	// change the thumbnail image url to large image url
	// the thumbnail image url like : http://ecx.images-amazon.com/images/I/41rE7imwXpL._SL75_AA30_.jpg
	// the large image url is: http://ecx.images-amazon.com/images/I/41rE7imwXpL.jpg
	private String getImageUrl(String urlThumb) {
		int last = urlThumb.lastIndexOf(".");
		String ext = urlThumb.substring(last);
		int start = urlThumb.lastIndexOf(".",last-1);
		if(start>-1){
			String imageUrl = urlThumb.substring(0,start)+ext;
			return imageUrl;
		}else{
			return urlThumb;
		}
	}

	private List parseReviews(String pid,Document doc) {
		List<Object> reviews = new ArrayList<Object>();
		String reviewUrl = this.selectLink(doc, ".asinReviewsSummary a:contains(reviews)");
		if(reviewUrl == null){
			return null;
		}
		int index= 0;
		while(reviewUrl!=null){
			String content = download(reviewUrl);
			Document reviewDoc = parseHtmlDocument(content);
			if(reviewDoc==null){
				break;
			}
			try{
				Elements select = reviewDoc.select("#productReviews td>div");
				for(int i=0;i<select.size();i++){
					Element el = select.get(i);
					Element elStar = el.select(".swSprite").first();
					if(elStar==null){
						continue;
					}
					String star =elStar.text();
					Element firstDiv = getParent(elStar,"div");
					String title = this.selectText(firstDiv, "b");
					String date = this.selectText(firstDiv, "nobr");
					Element a = firstDiv.nextElementSibling().select("a").first();
					String rstUrl = this.selectLink(el, ".tiny a:contains(Permalink)");
					if(StringUtils.isEmpty(rstUrl)){
						continue;
					}
					String reviewId = getReviewId(rstUrl);
					if(StringUtils.isEmpty(reviewId)){
						continue;
					}
					String authorName = "";
					String authorUrl = "";
					if(a!=null){
						authorName = a.text();
						authorUrl = a.attr("href");
					}
					List<Node> childNodes = el.childNodes();
					StringBuilder reviewContent = new StringBuilder();
					for (Node child : childNodes) {
			            if (child instanceof TextNode) {
			            	reviewContent .append(((TextNode) child).text());
			            }
					}
					Map<String,Object> review= new HashMap<String,Object>();
					review.put("rstrId", reviewId);
					review.put(Constants.OBJECT_ID, UUIDUtils.getMd5UUID(reviewId));
					review.put("reviewUrl", rstUrl);
					review.put("pid", pid);
					review.put("star", star);
					review.put("title", title);
					review.put("datetime", date);
					review.put("authorName", authorName);
					review.put("authorUrl", authorUrl);
					review.put("reviewContent", reviewContent.toString().trim());
					review.put(Constants.FETCH_TIME, new Date());
					reviews.add(review);
					index++;
				}
				Element next = reviewDoc.select(".CMpaginate .paging a:contains(Next)").first();
				if(next!=null){
					reviewUrl =  next.attr("href");
				}else{
					reviewUrl = null;
				}
			}catch(Exception e){
				log.error("parse review exception index "+index+" at "+reviewUrl,e);
			}
		}
		return reviews;
	}

	private String getReviewId(String rurl){
		Pattern shopPattern = Pattern.compile("^http://www\\.amazon\\.com/review/(.*)/ref.*");
    	Matcher matcher = shopPattern.matcher(rurl);
		if(matcher.matches()){
			String group = matcher.group(1);
			return group;
		}
		return null;
	}
	public List<Map<String,Object>> parseTopCategoryList(String content){
		Document doc = parseHtmlDocument(content);
		if(doc==null){
			return null;
		}
		JSONArray categories = new JSONArray();
		Elements els = doc.select(".popover-grouping");
		if( els!=null &&  els.size()>0){
			Iterator<Element> iterator = els.iterator();
			while(iterator.hasNext()){
				Element el = iterator.next();
				String topCate = this.selectText(el, ".popover-category-name");
				Elements cates = el.select("a");
				Iterator<Element> cateItor = cates.iterator();
				while(cateItor.hasNext()){
					Element cate = cateItor.next();
					//Element first = cate.select("a").first();
					if(cate!=null){
						String text = cate.text();
						String link = cate.attr("href");
						JSONObject json = new JSONObject();
						json.put(Constants.QUEUE_KEY, topCate+"/"+text);
						json.put(Constants.QUEUE_NAME, topCate+"/"+text);
						json.put(Constants.QUEUE_URL, AmazonHost+link);
						json.put(Constants.QUEUE_DEPTH, "1");
						json.put(Constants.QUEUE_DISPOSE,"0");
						categories.add(json);
					}
				}
			}
		}
		return categories;
	}
	
	private boolean isMatch(Pattern pattern,String input){
		Matcher matcher = pattern.matcher(input);
		return matcher.matches();
	}
	
	private String getProductId(String url){
		Matcher matcher = ProductUrlPattern.matcher(url);
		if(matcher.matches()){
			String group = matcher.group(2);
			return group;
		}
		return "";
	}
	
	
	private String getUrl(String url){
		if(url==null){
			return "";
		}
		if(url.startsWith("/")){
			url = AmazonHost+url;
		}
//		QueryUrl query = UrlUtils.parseQueryUrlString(url);
//		query.removeParameter("pf_rd_r");
//		query.removeParameter("pf_rd_m");
//		url = query.toString();
		return url;
	}
	
	public String download(String url){
		String content = null;
		while(true){
			content = WebPageDownloader.download(url);
			if(StringUtils.isEmpty(content)){
				log.warn("*********获取内容为空,正在等待,5秒后继续访问...");
				sleep(5);
				return null;
			}
			if(content.contains("500 Service Unavailable Error")){
				log.warn("*********访问频率太快，正在等待，5秒后继续访问...");
				sleep(5);
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
	
	private boolean isAmazonHostLink(Map<String,Object> meta){
		String url = (String)meta.get("url");
		if(StringUtils.isEmpty(url)){
			return false;
		}
		return url.startsWith(AmazonHost);
	}
}
