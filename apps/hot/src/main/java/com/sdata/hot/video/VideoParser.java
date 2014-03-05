package com.sdata.hot.video;

import java.util.Date;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.hot.Hot;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class VideoParser extends SdataParser{
	
	private int count;
	
	public VideoParser(Configuration conf){
		this.count = conf.getInt("crawl.count", 3);
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
		try {
			Document doc = getDocFromStr(content);
			Element feed = (Element)doc.selectSingleNode("//feed");
			List<Element> elements = feed.elements("entry");
			for(int i=1;i<=elements.size()&&i<=count;i++){
				FetchDatum datum = new FetchDatum();
				Element e = elements.get(i-1);
				String title = e.elementText("title");
				String pubDate = e.elementText("published");
				String image = e.element("group").element("thumbnail").attributeValue("url");
				String contet = e.element("group").elementText("description");
				String target = e.element("group").element("player").attributeValue("url");
				String id = e.element("group").elementText("videoid");
				String uname = e.element("author").elementText("name");
				String views = e.element("statistics").attributeValue("viewCount");
				List eles = e.elements("rating");
				if(eles!=null&&eles.size()==2){
					String likes = ((Element)eles.get(1)).attributeValue("numLikes");
					String dislikes = ((Element)eles.get(1)).attributeValue("numDislikes");
					String comments = e.element("comments").element("feedLink").attributeValue("countHint");
					datum.addMetadata("likes", likes);
					datum.addMetadata("dislikes", dislikes);
					datum.addMetadata("comments", comments);
				}
				String uuri = e.element("author").elementText("uri");
				RawContent rc = HotUtils.getRawContent(uuri);
				Document udoc = getDocFromStr(rc.getContent());
				Element user =(Element)udoc.selectSingleNode("//entry");
				String head = user.element("thumbnail").attributeValue("url");

				byte[] rk = HotUtils.getRowkey(Hot.Video.getValue(), fetTime, i);
				datum.addMetadata("rk", rk);
				datum.addMetadata("rank", i);
				datum.addMetadata("id", id);
				datum.addMetadata("type", Hot.Video.getValue());
				datum.addMetadata("fet_time", fetTime);
				datum.addMetadata("target", target);
				datum.addMetadata("title", title);
				datum.addMetadata("content", contet);
				datum.addMetadata("image", image);
				datum.addMetadata("pub_time", DateFormat.strToDate(pubDate));
				datum.addMetadata("uname", uname);
				datum.addMetadata("views", views);
				datum.addMetadata("head", head);
				datum.setUrl(target);
				result.addFetchDatum(datum);
			}
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	protected Document getDocFromStr(String str) throws DocumentException{
		 return DocumentHelper.parseText(str);  
	}
}
