package com.sdata.component.parser;


import java.math.BigDecimal;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.Location;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.LinkUtils;
import com.sdata.core.util.WebPageDownloader;

public class PanoramioParser extends SdataParser {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.PanoramioParser");
	
	public PanoramioParser(Configuration conf,RunState state){
		setConf(conf);
	}

	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		String count = StringUtils.valueOf(JSObj.get("count"));
		if(StringUtils.isEmpty(count) || count.equals("0")){
			return result;
		}
		JSONArray photoArr = (JSONArray)JSObj.get("photos");
		if(photoArr!=null){
			Iterator<JSONObject> photoIterator= photoArr.iterator();
			while(photoIterator.hasNext()){
				JSONObject photoObj = photoIterator.next();
				String photoId = StringUtils.valueOf(photoObj.get("photo_id"));
				String photoUrl = StringUtils.valueOf(photoObj.get("photo_url"));
				String uploadDate = StringUtils.valueOf(photoObj.get("upload_date"));
				String photoFileUrl = StringUtils.valueOf(photoObj.get("photo_file_url"));
				FetchDatum datum = new FetchDatum();
				datum.setId(photoId);
				datum.setUrl(photoUrl);
				datum.addMetadata("uploadDate", uploadDate);
				datum.addMetadata("imageUrl", photoFileUrl);
				result.addFetchDatum(datum);
			}
		}
		return result;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		String cityName = this.getConf("CityName");
		String refPageContent= c.getContent();
		if(StringUtils.isEmpty(refPageContent)){
			return null;
		}
		Document doc = Jsoup.parse(refPageContent);
		ParseResult result = new ParseResult();
		JSONObject jobj= new JSONObject();
		String photoUrl = c.getUrl();
		jobj.put("referenceUrl",photoUrl);
		jobj.put("title",selectText(doc,ImageTitleSelector));
		jobj.put("description","");
		
		String originalPhotoId = getOriginalPhotoId(photoUrl);
		jobj.put("originalPhotoId",originalPhotoId);
		String imagelink = toString( selectLink(doc, ImageFileUrlSelector) );
		jobj.put("imageUrl",imagelink);
		jobj.put("fileType",LinkUtils.getLinkFileType(imagelink));
		jobj.put("siteType","panoramio");
		jobj.put("cityName", cityName);
		Location location = parseLocation(doc);
		if(location!=null){
			jobj.put("latitude",StringUtils.valueOf(location.getLatitude()));
			jobj.put("longitude",StringUtils.valueOf(location.getLongitude()) );
		}
		jobj.put("imageTags",this.parseTags(doc));
		this.parseAuthorInfo(jobj,doc);
		// get the comments
		this.parseComments(jobj, doc);
		this.parseImageExif(jobj, doc);
		result.setMetadata(jobj);
		return result;
	}
	
	private static final String ImageTextTagSelector = "#main #map-data #photo-tags #tags li a";
	private static final String ImageCommentsSelector ="#main #comments_wrapper div.comment";
	private static final String ImageCommentCurrentPageSelector ="#main #comments_wrapper div.pages .selected";
	private static final String ImageTitleSelector = "#main #photo-title";
	private static final String ImageFileUrlSelector ="#main #main-photo img";
	private static final String ImageLocationSelector = "#main #map-data #location .geo";
	private static final String ImageAuthorSelector = "#main #author";
	private static final String ImageExifSelector = "#main #photo-info #details #tech-details ul li";
	
	public static Object sync=new Object();
//	
//	@Override
//	public boolean resolveImage(AbstractTask task, PageRegionArea currentRegionArea,
//			String content) {
//		if(StringUtil.isEmpty(content)){
//			return false;
//		}
////		System.out.println(currentRegionArea.toString());
//		Object obj = JSONValue.parse(content);
//		JSONObject contentObj = (JSONObject)obj;
//		boolean havemore = toBoolean(contentObj.get("has_more"));
//		JSONArray photos = (JSONArray)contentObj.get("photos");
//		@SuppressWarnings("unchecked")
//		Iterator<JSONObject> iterator= photos.iterator();
//		Long taskId = task.getTaskId();
//		String currentCityName = task.getState().getCurrentCityName();
//		while(iterator.hasNext()){
//			JSONObject imageObj = iterator.next();
//			Image image = this.resolveOneImage(imageObj);
//			if(image==null){
//				continue;
//			}
//			image.setCurrentRegionArea(currentRegionArea);
//			image.setCityName(currentCityName);
//			image.setState(task.getState());
//			dispatch.dispatch(taskId,image);
//			if(task.getStop()){
//				break;
//			}
//		}
//		return havemore;
//	}
//	
//	private Image resolveOneImage(JSONObject imageObj) {
//		String referenceUrl = toString(imageObj.get("photo_url"));
//		Image image  = new Image();
//		image.setReferenceUrl(referenceUrl);
//		image.setTitle(toString(imageObj.get("photo_title")));
//		image.setDescription("");
//		String photoId=toString(imageObj.get("photo_id"));
//		image.setOriginalPhotoId("pnm_"+photoId);
//		String imagelink = toString(imageObj.get("photo_file_url"));
//		image.setImageUrl(imagelink);
//		image.setFileType(LinkUtils.getLinkFileType(imagelink));
//		image.setSiteType(DataWebSiteType.Panoramio);
//		image.setLatitude(toBigdecimal(imageObj.get("latitude")));
//		image.setLongitude(toBigdecimal(imageObj.get("longitude")));
//		image.setWidth(toInt(imageObj.get("width")));
//		image.setHeight(toInt(imageObj.get("height")));
//		image.setOwnerId(toString(imageObj.get("owner_id")));
//		image.setOwnerName(toString(imageObj.get("owner_name")));
//		return image;
//	}
//	
//	/**
//	 * this method be used to get some other informations that be contained in the webpage
//	 * @param image
//	 */
//	public Image resolveOneImage(Image image){
//		String referenceUrl = image.getReferenceUrl();
//		String refPageContent=WebPageDownloader.download(referenceUrl);
//		if(StringUtil.isEmpty(refPageContent)){
//			return null;
//		}
//		Document doc = Jsoup.parse(refPageContent);
//		// get the image tags
//		image.setImageTags(this.resolveTags(doc));
//		// get the comments
//		this.resolveComments(image, doc);
//		this.resolveImageExif(image, doc);
//		return image;
//	}
//	
//	public Image resolveOneImage(String imagePageUrl){
//		
//	}
//	
	private String getOriginalPhotoId(String url){
		if(url.endsWith("/")){
			url = url.substring(0,url.length()-1);
		}
		int start = url.lastIndexOf("/");
		String photoId = url.substring(start+1);
		return photoId;
	}
	
	protected Location parseLocation(Document doc){
		Elements locationEle=doc.select(ImageLocationSelector);
		if(locationEle!=null){
			Location location = new Location();
			String latitude=locationEle.select(".latitude").attr("title");
			String longitude = locationEle.select(".longitude").attr("title");
			location.setLatitude(new BigDecimal(latitude));
			location.setLongitude(new BigDecimal(longitude));
			return location;
		}
		return null;
	}
	
	protected void parseAuthorInfo(JSONObject jobj,Document doc){
		Elements nameElements = doc.select(ImageAuthorSelector);
		if(nameElements==null){
			return;
		}
		Element nameAnchor = nameElements.select("a").first();
		if(nameAnchor!=null){
			String name=nameAnchor.text();
			jobj.put("ownerName",name);
			if(!jobj.containsKey("ownerId")){
				String href = nameAnchor.attr("href");
				///user/2900405?with_photo_id=41523618"
				String pattern_id="/user/(.*)\\?with_photo_id=.*";
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

	private JSONArray parseTags(Document doc) {
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
		return tags;
	}

	/**
	 * resolve the image comments information
	 * @param doc
	 */
	private void parseComments(JSONObject jobj,Document doc){
		Elements selected = doc.select(ImageCommentsSelector);
		Collections.reverse(selected);
		Iterator<Element> iterator = selected.iterator();
		//Long imageId= jobj.getImageId();
		JSONArray comments = jobj.containsKey("imageComments")?(JSONArray)jobj.get("imageComments"):null;
		if(comments==null){
			comments = new JSONArray();
		}
		while(iterator.hasNext()){
			JSONObject comment =new JSONObject();
			Element el = iterator.next();
			String comment_txt = el.select(".photo-comment-text").text();
			comment.put("comment",comment_txt);
			//comment.put("ImageId",imageId);
			Elements author = el.select(".comment-author a");
			String authorName = author.text();
			String authorId_href = author.attr("href");
			String authorId = authorId_href.substring(authorId_href.lastIndexOf("/")+1);
			comment.put("ownerId",authorId.trim());
			comment.put("ownerName",authorName);
			comments.add(comment);
		}
		// get more comment pages
		String referenceUrl = (String)jobj.get("referenceUrl");
		String currentPage = doc.select(ImageCommentCurrentPageSelector).text();
		if(StringUtils.isNum(currentPage)){
			Long page= Long.valueOf(currentPage);
			if(page>1){
				page--;
				String newCommentPageUrl=referenceUrl+"?comment_page="+page;
				String html=WebPageDownloader.download(newCommentPageUrl);
				if(StringUtils.isEmpty(html)){
					return;
				}
				Document newdoc = Jsoup.parse(html);
				parseComments(jobj,newdoc);
			}
		}
		jobj.put("imageComments", comments);
	}
	
	private void parseImageExif(JSONObject jobj,Document doc){
		Elements selected = doc.select(ImageExifSelector);
		if(selected.size()==8){
			JSONObject exif= new JSONObject();
			//exif.setImageId(jobj.getImageId());
			exif.put("camera",getPatternText("Camera:(.*)",selected.get(0),1));
			exif.put("dateTime", getPatternText("Taken on(.*)",selected.get(1),1));
			exif.put("exposureTime",getPatternText("Exposure:.*\\((.*)\\)",selected.get(2),1));
			exif.put("focalLength",getPatternText("Focal Length: (.*)mm",selected.get(3),1));
			exif.put("fNumber",getPatternText("F/Stop:(.*)",selected.get(4),1));
			exif.put("isoSpeed",getPatternText("ISO Speed: ISO(.*)",selected.get(5),1));
			exif.put("exposureBias",getPatternText("Exposure Bias:(.*)",selected.get(6),1));
			exif.put("flash",selected.get(7).text());
			if(exif.size()>0){
				jobj.put("imageExif", exif);
			}
		}
	}
	
	private String getPatternText(String pattern,Element el,int group){
		String text = el.text();
		Pattern compile = Pattern.compile(pattern);
		Matcher matcher = compile.matcher(text);
		if(matcher.matches()){
			String groupText = matcher.group(group);
			return groupText.trim();
		}
		return "";
	}
}
