package com.sdata.hot.fetcher.video.youtube;

import java.util.ArrayList;
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
import com.sdata.hot.HotConstants;
import com.sdata.hot.Source;
import com.sdata.hot.fetcher.video.HotVideoFetcher;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class YoutubeFetcher extends HotVideoFetcher{
	
	private int count;
	private String HOST = "http://gdata.youtube.com/feeds/api/standardfeeds/SG/most_popular?time=today";
	private String YouTube = "http://www.youtube.com/v/";
	
	public YoutubeFetcher(){
		
	}
	public YoutubeFetcher(Configuration conf){
		this.count = conf.getInt("crawl.count", 3);
	}
	
	@Override
	public List<FetchDatum> getDatumList() {
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		RawContent c = HotUtils.getRawContent(HOST);
		if(c == null){
			throw new NegligibleException("RawContent is null!");
		}
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			throw new NegligibleException("init web content is empty!");
		}
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
					if(e.element("comments") != null){
						String comments = e.element("comments").element("feedLink").attributeValue("countHint");
						datum.addMetadata("comments", comments);
					}
					datum.addMetadata("likes", likes);
					datum.addMetadata("dislikes", dislikes);
				}
				String uuri = e.element("author").elementText("uri");
				RawContent rc = HotUtils.getRawContent(uuri);
				Document udoc = getDocFromStr(rc.getContent());
				Element user =(Element)udoc.selectSingleNode("//entry");
				String head = user.element("thumbnail").attributeValue("url");

				byte[] rk = super.getHotRowKeyBytes(i);
				datum.addMetadata(HotConstants.ROWKEY, rk);
				datum.addMetadata(HotConstants.RANK, i);
				datum.addMetadata(HotConstants.BATCH_TIME, super.getBatchTime());
				datum.addMetadata(HotConstants.ID, id);
				datum.addMetadata(HotConstants.TYPE, type().getValue());// type
				datum.addMetadata(HotConstants.SOURCE, source().getValue());// source
				datum.addMetadata(HotConstants.FETCH_TIME, new Date());
				
				datum.addMetadata("target", target);
				datum.addMetadata("title", title);
				datum.addMetadata("content", contet);
				datum.addMetadata("image", image);
				datum.addMetadata("pub_time", DateFormat.changeStrToDate(pubDate));
				datum.addMetadata("uname", uname);
				datum.addMetadata("views", views);
				datum.addMetadata("head", head);
				datum.addMetadata("video", YouTube.concat(id));
				datum.setUrl(target);
				result.add(datum);
			}
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	protected Document getDocFromStr(String str) throws DocumentException{
		 return DocumentHelper.parseText(str);  
	}

	public Source source() {
		return Source.Youtube;
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		return datum;
	}
}
