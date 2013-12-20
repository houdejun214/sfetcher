package com.sdata.hot.event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.core.util.JsoupUtils;
import com.sdata.hot.Hot;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
@Deprecated
public class EventParser extends SdataParser{
	
	private int eventCount;
	
	public EventParser(Configuration conf){
		this.eventCount = conf.getInt("crawl.count", 3);
	}
	
	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
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
		for(int i=1;i<=eventCount&&i<=list.size();i++){
			FetchDatum datum = new FetchDatum();
			Element e = list.get(i-1);
			String link = JsoupUtils.getLink(e, "a");
			byte[] rk = HotUtils.getRowkey(Hot.Event.getValue(), fetTime, i);
			datum.addMetadata("rk", rk);
			datum.addMetadata("rank", i);
			datum.addMetadata("id", link.hashCode());
			datum.addMetadata("type", Hot.Event.getValue());
			datum.addMetadata("fet_time", fetTime);
			datum.addMetadata("target", link);
			datum.setUrl(link);
			result.addFetchDatum(datum);
		}
		return result;
	}
	
	public void parse(FetchDatum datum) {
		if(datum== null||StringUtils.isEmpty(datum.getUrl())){
			throw new NegligibleException("event parser datum url is null!");
		}
		Document doc = DocumentUtils.getDocument(datum.getUrl());
		String title = JsoupUtils.getText(doc, "#contentcolumn .headline32");
		String image = JsoupUtils.getLink(doc, "#contentcolumn .article_img img");
		datum.addMetadata("title", title);
		datum.addMetadata("image", image);
		String extras = JsoupUtils.getText(doc, "#contentcolumn .article_extras");
		String[] ets = extras.split("\\|");
		if(ets.length>=1){
			String pub = ets[0].replaceAll("Posted on", "").trim();
			datum.addMetadata("pub_time", DateFormat.strToDate(trim(pub)));
		}
		if(ets.length>=2){
			String views = ets[1].replaceAll("views", "").replaceAll(",", "").trim();
			datum.addMetadata("views", trim(views));
		}
		if(ets.length>=3){
			String comments = ets[2].replaceAll("comments", "").replaceAll(",", "").trim();
			datum.addMetadata("comments", trim(comments));
		}
		if(ets.length>=4){
			String shares = ets[3].replaceAll("shares", "").replaceAll(",", "").trim();
			datum.addMetadata("shares", trim(shares));
		}
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
}
