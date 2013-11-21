package com.sdata.hot.fetcher.video.stomp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import net.sf.json.JSONObject;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.core.util.JsoupUtils;
import com.sdata.hot.HotConstants;
import com.sdata.hot.Source;
import com.sdata.hot.fetcher.video.HotVideoFetcher;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class StompFetcher extends HotVideoFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.StompFetcher");
	
	private String HOST = "http://singaporeseen.stomp.com.sg/singaporeseen/category/hot-topics";
	private int count;
	private static final String REGEX ="(\\{.*\\})";
	
	public StompFetcher() {
	}
	public StompFetcher(Configuration conf) {
		super(conf);
		this.count = conf.getInt("crawl.count", 3);
	}

	public List<FetchDatum> getDatumList() {
		RawContent c = HotUtils.getRawContent(HOST);
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		if(c == null){
			throw new NegligibleException("RawContent is null!");
		}
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			throw new NegligibleException("init web content is empty!");
		}
		Date fetTime = new Date();
		Document doc = DocumentUtils.parseDocument(content,c.getUrl());
		List<Element> list = JsoupUtils.getList(doc, ".views-row .headline20");
		for(int i=1;i<=list.size()&&result.size()<count;i++){	
			Element e = list.get(i-1);
			String link = JsoupUtils.getLink(e, "a");
			FetchDatum datum = this.getDatum(link);
			if(datum == null){
				continue;
			}
			byte[] rk = getHotRowKeyBytes(i);
			datum.addMetadata(HotConstants.ROWKEY, rk);
			datum.addMetadata(HotConstants.RANK, i);
			datum.addMetadata(HotConstants.ID, link.hashCode());
			datum.addMetadata(HotConstants.TYPE, type().getValue());
			datum.addMetadata(HotConstants.SOURCE, source().getValue());
			datum.addMetadata(HotConstants.FETCH_TIME, fetTime);
			datum.addMetadata(HotConstants.BATCH_TIME, super.getBatchTime());
			datum.addMetadata("target", link);
			datum.setUrl(link);
			result.add(datum);
		}
		return result;
	}

	public Source source() {
		return Source.Stomp;
	}

	public FetchDatum getDatum(String url) {
		FetchDatum datum = new FetchDatum();
		if(StringUtils.isEmpty(url)){
			throw new NegligibleException("event parser datum url is null!");
		}
		Document doc = DocumentUtils.getDocument(url);
		
		String videoScript = doc.select(".field-collection-container script").html();
		if(StringUtils.isEmpty(videoScript)){
			return null;
		}
		String matchPattern = PatternUtils.getMatchPattern("(\\{.*\\})", videoScript.replaceAll("\r\n", "").trim(), 1);
		JSONObject json = JSONObject.fromObject(matchPattern);
		datum.addMetadata("video", json.get("file"));
		String title = JsoupUtils.getText(doc, "#contentcolumn .headline32");
		String image = JsoupUtils.getLink(doc, "#contentcolumn .article_img img");
		datum.addMetadata("title", title);
		datum.addMetadata("image", image);
		String extras = JsoupUtils.getText(doc, "#contentcolumn .article_extras");
		String[] ets = extras.split("\\|");
		if(ets.length>=1){
			String pub = ets[0].replaceAll("Posted on", "").trim();
			datum.addMetadata("pub_time", DateFormat.changeStrToDate(trim(pub)));
		}
		if(ets.length>=2){
			String views = ets[1].replaceAll("views", "").trim();
			datum.addMetadata("views", trim(views));
		}
		if(ets.length>=3){
			String comments = ets[2].replaceAll("comments", "").trim();
			datum.addMetadata("comments", trim(comments));
		}
		if(ets.length>=4){
			String shares = ets[3].replaceAll("shares", "").trim();
			datum.addMetadata("shares", trim(shares));
		}
		return datum;
	}
	
	private String trim(String str){
		byte[] bytes = str.getBytes();
		List<Byte> remove = Arrays.asList((byte)-62,(byte)-96,(byte)-62,(byte)-96);
		List<Byte> result = new ArrayList<Byte>();
		for(byte b:bytes){
			result.add(b);
		}
		while(result.containsAll(remove)){
			result.removeAll(remove);
		}
		byte[] sb = new byte[result.size()];
		for(int i=0;i<result.size();i++){
			sb[i] = result.get(i);
		}
		return new String(sb);
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		return datum;
	}
}
