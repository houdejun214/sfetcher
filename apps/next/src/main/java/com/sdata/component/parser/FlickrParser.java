package com.sdata.component.parser;


import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.Location;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.LinkUtils;
import com.sdata.core.util.WebPageDownloader;


public class FlickrParser extends SdataParser{
	
	protected static final String SearchListSelector="#ResultsContainer .ResultsThumbs div.ResultsThumbsChild span.photo_container a";
	protected static final String ImageTitleSelector="#main #meta h1.photo-title";
	protected static final String ImageDescriptionSelector="#main #meta .photo-desc";
	protected static final String ImageSelector="#main #photo .photo-div img";
	protected static final String ImageTextTagSelector = "#main #sidebar #sidecar #thetags-wrapper #thetags li span.tag-wrapper a";
	protected static final String ImageLocationSelector="#main #sidebar #photo-story #photo-story-map img";
	protected static final String ImageAuthorSelector = "#main #sidebar #photo-story #photo-story-attribution .flickr-user .name";
	protected static final String ImageOverlayTagSelector = "#main #photo #photo-drag-proxy #notes li.note .note-content .note-wrap";
	protected static final String ImageCommentsSelector ="#main #comments ol li.comment-block";
	protected static final String ImageCommentCurrentPageSelector ="#main #comments .Pages .this-page";
	protected static final String ImageGroupsSelector = "#main #sidebar-contexts li[data-context-group_id]";
	
	public static final String FlickrPrefix = "http://www.flickr.com";
	
	public static final Log log = LogFactory.getLog("SdataCrawler.FlickrParser");
	
	public FlickrParser(Configuration conf,RunState state){
		setConf(conf);
	}

	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		org.dom4j.Document doc = parseXmlDocument(c);
		org.dom4j.Element rootElement = doc.getRootElement();
		String stat = rootElement.attributeValue("stat");
		if(!"ok".equals(stat)){
			org.dom4j.Element errorelement = rootElement.element("err");
			throw new RuntimeException("web response stat is failed:"+errorelement.attributeValue("msg"));
		}
		org.dom4j.Element photos = rootElement.element("photos");
		if(photos==null){
			return result;
		}
		@SuppressWarnings("rawtypes")
		Iterator elementIterator = photos.elementIterator("photo");
		if(elementIterator==null || !elementIterator.hasNext()){
			return result;
		}
		while(elementIterator.hasNext()){
			org.dom4j.Element photo = (org.dom4j.Element)elementIterator.next();
			String photoId=photo.attributeValue("id");
			String originalId = photoId;
			String owner=photo.attributeValue("owner");
			String datetaken=photo.attributeValue("datetaken");
			String dateupload=photo.attributeValue("dateupload");
			String photourl = "http://www.flickr.com/photos/"+owner+"/"+photoId+"/";
			FetchDatum datum = new FetchDatum();
			datum.setId(originalId);
			datum.setUrl(photourl);
			datum.addMetadata("takenDate", datetaken);
			datum.addMetadata("uploadDate", dateupload);
			result.addFetchDatum(datum);
		}
		return result;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		Document doc = parseHtmlDocument(c);
		if(doc==null){
			return null;
		}
		JSONObject jobj= new JSONObject();
		jobj.put("referenceUrl",c.getUrl());
		jobj.put("title",selectText(doc,ImageTitleSelector));
		jobj.put("description",selectText(doc,ImageDescriptionSelector));
		String imagelink = selectLink(doc,ImageSelector);
		if(StringUtils.isEmpty(imagelink)){
			throw new RuntimeException("this isn't a photo page, it is maybe a video or other page!");
		}
		imagelink = correctImageFileUrl(imagelink);
		String originalPhotoId = getOriginalPhotoId(imagelink);
		jobj.put("originalPhotoId",originalPhotoId);
		jobj.put(Constants.OBJECT_ID, Long.valueOf(originalPhotoId));
		jobj.put("imageUrl",imagelink);
		jobj.put("fileType",LinkUtils.getLinkFileType(imagelink));
		jobj.put("siteType","flickr");
		Location location = this.parseLocation(doc);
		if(location!=null){
			jobj.put("latitude",StringUtils.valueOf(location.getLatitude()));
			jobj.put("longitude",StringUtils.valueOf(location.getLongitude()));
		}
		jobj.put("tags",this.parseTags(doc));
		this.parseAuthorInfo(jobj,doc);
		this.parseComments(jobj, doc, originalPhotoId);
		this.parseGroups(jobj, doc);
		this.parseExif(jobj, c.getUrl());
		result.setMetadata(jobj);
		return result;
	}

	/**
	 * get the comments
	 */
	protected void parseComments(JSONObject jobj,Document doc, String originalPhotoId){
		Elements selected = doc.select(ImageCommentsSelector);
		if(selected.size()<=0){
			return;
		}
		Iterator<Element> iterator = selected.iterator();
		JSONArray comments = jobj.containsKey("comments")?jobj.getJSONArray("comments"):null;
		if(comments==null){
			comments = new JSONArray();
		}
		while(iterator.hasNext()){
			JSONObject comment =new JSONObject();
			Element el = iterator.next();
			String comment_txt = el.select(".comment-body").text();
			comment.put("comment",comment_txt);
			Elements author = el.select(".comment-head .comment-owner a.comment-author");
			String authorName = author.text();
			String authorId_href = author.attr("href");
			authorId_href=StringUtils.trimTrailingCharacter(authorId_href, '/');
			String authorId = authorId_href.substring(authorId_href.lastIndexOf("/")+1);
			comment.put("ownerId",authorId.trim());
			comment.put("ownerName",authorName);
			Elements date = el.select(".comment-date a");
			String date_str = date.attr("title");
			if(!StringUtils.isEmpty(date_str)){
				//Date commentTime = new Date(date_str);
				comment.put("commentTime",date_str);
			}
			try {
				Long pid = Long.valueOf(originalPhotoId);
				comment.put("pid", pid);
			} catch (NumberFormatException e) {
				comment.put("pid", originalPhotoId);
			}
			comments.add(comment);
		}
		Elements currentPage = doc.select(ImageCommentCurrentPageSelector);
		String txt_curpage = currentPage.text();
		if(!StringUtils.isEmpty(txt_curpage)){
			Long curPage=Long.valueOf(txt_curpage);
			Elements parents = currentPage.parents();
			if(parents.select(".AtEnd").size()>0){
				return;
			}
			// get the next page's comments
			Long nextPage = curPage+1;
			String referenceUrl = jobj.get("referenceUrl").toString();
			if(!referenceUrl.endsWith("/")){
				referenceUrl=referenceUrl+"/";
			}
			String newCommentPageUrl=referenceUrl+"page"+nextPage;
			String html=WebPageDownloader.download(newCommentPageUrl);
			Document newdoc = Jsoup.parse(html);
			parseComments(jobj,newdoc, originalPhotoId);
		}
		jobj.put("comments", comments);
	}
	
	protected void parseGroups(JSONObject jobj,Document doc){
		JSONArray groups = new JSONArray();
		Iterator<Element> iterator = doc.select(ImageGroupsSelector).iterator();
		while(iterator.hasNext()){
			Element el = iterator.next();
			String groupId = el.attr("data-context-group_id");
			if(groupId==null ||"".equals(groupId)){
				continue;
			}
			groups.add(groupId);
		}
		jobj.put("groups", groups);
	}
	
	private String getOriginalPhotoId(String url){
		if(StringUtils.isEmpty(url)){
			return "";
		}
		String parttern="^.*/(\\d*)_.*(_[mstzb])?.(jpg|gif|png)(\\?.*)?$";
		Pattern compile = Pattern.compile(parttern);
		Matcher matcher = compile.matcher(url);
		if(matcher.matches()){
			return matcher.group(1);
		}
		return "";
	}
	
	protected void parseAuthorInfo(JSONObject jobj,Document doc){
		Elements nameElements = doc.select(ImageAuthorSelector);
		if(nameElements==null){
			return;
		}
		Element nameAnchor = nameElements.select(".username a").first();
		if(nameAnchor!=null){
			String name=nameAnchor.text();
			jobj.put("ownerName",name);
			if(!jobj.containsKey("ownerId")){
				String href = nameAnchor.attr("href");
				String pattern_id="/photos/(.*)/?";
				Pattern compile = Pattern.compile(pattern_id);
				Matcher matcher = compile.matcher(href);
				if(matcher.matches()){
					String authorId=matcher.group(1);
					if(authorId.endsWith("/")){
						authorId=authorId.substring(0, authorId.length()-1);
					}
					jobj.put("ownerId",authorId);
				}
			}
		}
		
	}
	
	protected String correctImageFileUrl(String url){
		if(StringUtils.isEmpty(url)){
			return "";
		}
		String parttern="^(.*_.*)(_[mstzb]).(jpg|gif|png)$";
		try {
			String value =url.replaceAll(parttern, "$1_z.$3");
			return value;
		} catch (Exception e) {
			return url;
		}
	}
	
	protected Location parseLocation(Document doc){
		String link=selectLink(doc, ImageLocationSelector);
		if(!StringUtils.isEmpty(link)){
			Location location = new Location();
			int startParamPosition=link.indexOf("?");
			String paramString = link.substring(startParamPosition+1);
			String[] params = paramString.split("&");
			HashMap<String, String> paramMap = new HashMap<String, String>();
			for(String param:params){
				String[] paramValue = param.split("=");
				paramMap.put(paramValue[0], paramValue[1]);
			}
			String c = StringUtils.valueOf(paramMap.get("c"));
			String[] geo = c.split(",");
			location.setLatitude(new BigDecimal(geo[0]));
			location.setLongitude(new BigDecimal(geo[1]));
			return location;
		}
		return null;
	}
	
	protected JSONArray parseTags(Document doc){
		// resolve the tags
		JSONArray tags = new JSONArray();
		Iterator<Element> iterator = doc.select(ImageTextTagSelector).iterator();
		while(iterator.hasNext()){
			Element el = iterator.next();
			String text = el.text();
			if(text==null ||"".equals(text) || text.startsWith("Show all tags")){
				continue;
			}
			tags.add(text);
		}
		// resolve the overlay tags 
		Iterator<Element> iter_overlay = doc.select(ImageOverlayTagSelector).iterator();
		while(iter_overlay.hasNext()){
			Element el = iter_overlay.next();
			String text = el.text();
			tags.add(text);
		}
		return tags;
	}
	
	protected void parseExif(JSONObject jobj,String referenceUrl){
		String exifPageUrl = StringUtils.trimTrailingCharacter(referenceUrl, '/')+"/meta/";
		Document newdoc = null;
		try {
			String html=WebPageDownloader.download(exifPageUrl);
			newdoc = Jsoup.parse(html);
		} catch (Exception e) {
			log.info("haven't Exif information of page ["+referenceUrl+"]");
			return;
		}
		Elements els = newdoc.select("#main .photo-data table");
		if(els.size()<=1){
			return;
		}
		JSONObject exif= new JSONObject();
		Element table = els.get(1);
		Elements trs = table.select("tr");
		for(Element el: trs){
			String title = this.selectText(el,"th");
			String value = this.selectText(el, "td");
			if("Aperture".equals(title)){
				exif.put("apertureValue", value);
			}else if("Date and Time (Original)".equals(title)){
				exif.put("dateTime", value);
			}else if("Exposure Bias".equals(title)){
				exif.put("exposureBias", value);
			}else if("Exposure".equals(title)){
				exif.put("exposureTime", value);
			//}else if("".equals(title)){
			//	exif.put("fNumber", value);
			}else if("Flash".equals(title)){
				exif.put("flash", value);
			}else if("Focal Length".equals(title)){
				exif.put("focalLength", value);
			}else if("Image Height".equals(title)){
				exif.put("imageHeight", value);
			}else if("Image Width".equals(title)){
				exif.put("imageWidth", value);
			}else if("ISO Speed".equals(title)){
				exif.put("isoSpeed", value);
			}else if("Camera".equals(title)){
				exif.put("camera", value);
			}
		}
		if(exif.size()>0){
			jobj.put("exif", exif);
		}
	}
}
