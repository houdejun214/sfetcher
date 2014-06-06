package com.sdata.apps.amazon.parser;


import com.lakeside.core.utils.StringUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.HtmlUtils;
import net.sf.json.JSONArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class AmazonParser extends SdataParser{
	
	public static final String AmazonHost="http://www.amazon.com";
	
	public static final Log log = LogFactory.getLog("SdataCrawler.AmazonParser");

	private static Pattern ProductUrlPattern = Pattern.compile("^http://www\\.amazon\\.com/(.*)/dp/(.*)/ref=(.*)$");

    private HttpPageLoader httpLoader = HttpPageLoader.getDefaultPageLoader();
	
	public AmazonParser(Configuration conf,RunState state){
		setConf(conf);
		this.state = state;
	}

	@Override
	public ParseResult parseList(RawContent c) {
		//String parentUrl = c.getUrl();
		int depth = toInt(c.getMetadata(Constants.QUEUE_DEPTH),1);
        String category = (String) c.getMetadata("category");
        String curUrl = c.getUrl();
		AmazonParseResult result = new AmazonParseResult();
		Document doc = parseHtmlDocument(c);
		if(doc==null){
			return result;
		}
        if (isBlocked(doc)){
            result.setBlock(true);
            return result;
        }
        if(doc.select("#main div.results,#searchResults div.results").size()==0){
            return result;
        }
        Elements products = doc.select("#rightContainerATF #rightResultsATF div.results div.prod" +
                ",#rightResultsATF #atfResults div.results.list div.product" +
                ",#rightResultsATF div.results.list div.product" +
                ",#rightResultsATF div.results div.prod");
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
        Element nextPager = doc.select(".pagnHy #pagnNextLink").first();
        if(nextPager!=null){
            String nextUrl = nextPager.attr("href");
            if(!nextUrl.startsWith("/")){
                nextUrl = curUrl + nextUrl;
            }else{
                nextUrl = AmazonHost + nextUrl;
            }
            result.setNextUrl(nextUrl);
        }
		return result;
	}

    private boolean isBlocked(Document doc) {
        if("Robot Check".equals(doc.title()) || "/errors/validateCaptcha".equals(doc.select("div.a-container form").attr("action"))){
            return true;
        }
        return false;
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
		jobj.put("productName", this.selectText(doc, ".parseasinTitle #btAsinTitle,#title,#aiv-content-title"));
		jobj.put("productId",pid);
		jobj.put("purl", c.getUrl());
		jobj.put("category", c.getMetadata("category"));
		jobj.put("isfetched", "1");
		jobj.put("reviewStar", this.selectText(doc, ".jumpBar .crAvgStars a span span"));
		jobj.put("listPrice", this.selectText(doc, "#listPriceValue"));
		jobj.put("price", this.selectText(doc,"#buybox .offer-price"));
		jobj.put("images", this.parseImageList(doc));
		jobj.put("stocksSatus", this.selectText(doc, ".buying span"));
		jobj.put("technicalDetails", HtmlUtils.filterExcludeTags(this.selectSiblingBlockContentHtml(doc, "#technical_details", ".content"),"img"));
		jobj.put("productDetails", HtmlUtils.filterExcludeTags(this.selectSiblingBlockContentHtml(doc, ".bucket h2:contains(Product Details)", ".content"),"img"));
		jobj.put("description",  HtmlUtils.filterExcludeTags(this.selectHtml(doc, "#productDescription .content,#productDescription .productDescriptionWrapper"),"img"));
		jobj.put("siteType", "amazon");
		jobj.put(Constants.CYCLE, state.getCycle());
		result.setMetadata(jobj);
		return result;
	}

	private JSONArray parseImageList(Document doc) {
		JSONArray images = new JSONArray();
        // only one image
        Element imageCell = doc.select("#main-image-container .image.selected img," +
                "#main-image-container #img-canvas img," +
                "#main-image-content #main-image-wrapper #main-image," +
                "#aiv-main-content .dp-img-bracket img").first();
        if(imageCell!=null){
            String src = imageCell.attr("src");
            String imageUrl = getImageUrl(src);
            images.add(imageUrl);
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
		return url;
	}
	
	public String download(String url){
		String content = null;
		while(true) {
            content = httpLoader.download(url).getContentHtml();
            if (StringUtils.isEmpty(content)) {
                log.warn("*********获取内容为空,正在等待,5秒后继续访问...");
                sleep(5);
                return null;
            }
            if (content.contains("500 Service Unavailable Error")) {
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
