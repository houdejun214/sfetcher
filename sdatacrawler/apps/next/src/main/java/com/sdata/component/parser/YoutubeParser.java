package com.sdata.component.parser;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gdata.data.Category;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.Link;
import com.google.gdata.data.Person;
import com.google.gdata.data.TextConstruct;
import com.google.gdata.data.extensions.Comments;
import com.google.gdata.data.extensions.FeedLink;
import com.google.gdata.data.extensions.Rating;
import com.google.gdata.data.media.mediarss.MediaCategory;
import com.google.gdata.data.media.mediarss.MediaContent;
import com.google.gdata.data.media.mediarss.MediaDescription;
import com.google.gdata.data.media.mediarss.MediaKeywords;
import com.google.gdata.data.media.mediarss.MediaPlayer;
import com.google.gdata.data.media.mediarss.MediaThumbnail;
import com.google.gdata.data.media.mediarss.MediaTitle;
import com.google.gdata.data.youtube.CommentEntry;
import com.google.gdata.data.youtube.CommentFeed;
import com.google.gdata.data.youtube.UserProfileEntry;
import com.google.gdata.data.youtube.VideoEntry;
import com.google.gdata.data.youtube.VideoFeed;
import com.google.gdata.data.youtube.YouTubeMediaGroup;
import com.google.gdata.data.youtube.YtStatistics;
import com.google.gdata.data.youtube.YtUserProfileStatistics;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;

public class YoutubeParser extends SdataParser {

	public static final Log log = LogFactory
			.getLog("SdataCrawler.TencentTopicParser");

	public YoutubeParser(Configuration conf, RunState state) {
		setConf(conf);
	}

	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		return result;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		return result;
	}
	
	public List<Map<String, Object>> parserYoutubeComments(CommentFeed commentFeed,String videoId) {
		List<Map<String, Object>> commentResults = new ArrayList<Map<String, Object>>();
		List<CommentEntry> comments = commentFeed.getEntries();
		if(comments!=null && comments.size()>0){
			for(CommentEntry comment:comments){
				Map<String, Object> commentMap = new HashMap<String, Object>();
				
				String origid = comment.getId();
				String[] spilts = origid.split(":");
				UUID id = UUIDUtils.getMd5UUID(spilts[spilts.length-1]);
				commentMap.put("origId", origid);
				commentMap.put(Constants.OBJECT_ID, id);
				
				DateTime published = comment.getPublished();
				this.getPublished(published, commentMap);
				
				DateTime updated = comment.getUpdated();
				this.getUpdated(updated, commentMap);
				
				Set<Category> categories = comment.getCategories();
				this.getCategory(categories, commentMap);
				
				TextConstruct title = comment.getTitle();
				if(title!=null){
					this.getTitle(title.getPlainText(), commentMap);
				}
				
				String content = comment.getPlainTextContent();
				if(content!=null){
					this.getContent(content, commentMap);
				}
				
				List<Link> links = comment.getLinks();
				this.getLink(links, commentMap);
				
				List<Person> authors = comment.getAuthors();
				this.getAuthor(authors, commentMap);
				
				commentMap.put("oVideoId", videoId);
				commentMap.put("videoId", UUIDUtils.getMd5UUID(videoId));
				commentResults.add(commentMap);
			}
		}
		
		return commentResults;
	}
	
	
	public Map<String ,Object> parserYoutubeUser(UserProfileEntry userProfileEntry){
		Map<String ,Object> userMap = new HashMap<String ,Object>();
		
		String userName = userProfileEntry.getUsername();
		userMap.put("userName", userName);
		this.getUserObjectId(userName, userMap);
		
		TextConstruct summary = userProfileEntry.getSummary();
		getSummary(userMap, summary);
		
		DateTime publishDate = userProfileEntry.getPublished();
		getPublished(publishDate, userMap);
		
		DateTime updateDate = userProfileEntry.getUpdated();
		getUpdated(updateDate, userMap);
		
		Set<Category> categories = userProfileEntry.getCategories();
		getCategory(categories, userMap);
		
		String title = userProfileEntry.getTitle().getPlainText();
		getTitle(title, userMap);
		
		String content = userProfileEntry.getContent()==null?"":userProfileEntry.getContent().getLang();
		getContent(content, userMap);
		
		List<Link> links = userProfileEntry.getLinks();
		getLink(links, userMap);
		
		List<Person> authors = userProfileEntry.getAuthors();
		getAuthor(authors, userMap);
		
		List<FeedLink> feedLinks = userProfileEntry.getFeedLinks();
		getListFeedLink(feedLinks,userMap);
		
		String firstName = userProfileEntry.getFirstName();
		userMap.put("firstName", firstName);
		
		if(userProfileEntry.getGender()!=null){
			String gender = userProfileEntry.getGender().name();
			userMap.put("gender", gender);
		}
		
		String lastName = userProfileEntry.getLastName();
		userMap.put("lastName", lastName);
		
		String location = userProfileEntry.getLocation();
		userMap.put("location", location);
		
		YtUserProfileStatistics statistics = userProfileEntry.getStatistics();
		getUserStatistics(statistics, userMap);
		
		MediaThumbnail thumbnail = userProfileEntry.getThumbnail();
		Map<String, Object> mediaThumbnailMap = this.getThumbnailMap(thumbnail);
		userMap.put("mediaThumbnail", mediaThumbnailMap);
		
		
		if(userProfileEntry.getAge()!=null){
			userMap.put("age", userProfileEntry.getAge());
		}
		
		if(StringUtils.isNotEmpty(userProfileEntry.getBooks())){
			userMap.put("books", userProfileEntry.getBooks());
		}
		
		if(StringUtils.isNotEmpty(userProfileEntry.getCompany())){
			userMap.put("company", userProfileEntry.getCompany());
		}
		
		if(StringUtils.isNotEmpty(userProfileEntry.getAboutMe())){
			userMap.put("aboutMe", userProfileEntry.getAboutMe());
		}
		
		if(StringUtils.isNotEmpty(userProfileEntry.getHobbies())){
			userMap.put("hobbies", userProfileEntry.getHobbies());
		}
		
		if(StringUtils.isNotEmpty(userProfileEntry.getHometown())){
			userMap.put("hometown", userProfileEntry.getHometown());
		}
		
		if(StringUtils.isNotEmpty(userProfileEntry.getMovies())){
			userMap.put("movies", userProfileEntry.getMovies());
		}
		
		if(StringUtils.isNotEmpty(userProfileEntry.getMusic())){
			userMap.put("music", userProfileEntry.getMusic());
		}
		
		if(StringUtils.isNotEmpty(userProfileEntry.getOccupation())){
			userMap.put("occupation", userProfileEntry.getOccupation());
		}
		
		if(StringUtils.isNotEmpty(userProfileEntry.getSchool())){
			userMap.put("school", userProfileEntry.getSchool());
		}
		
		return userMap;
	}

	private void getSummary(Map<String, Object> userMap, TextConstruct summary) {
		if(summary!=null){
			String text = summary.getPlainText();
			userMap.put("summary", text);
		}
	}
	
	
	

	public List<FetchDatum> parserYoutubeList(VideoFeed videoFeed,String feedName) {
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		List<VideoEntry> videoEntries = videoFeed.getEntries();
		if (videoEntries.size() == 0) {
			System.out.println("This feed contains no entries.");
			return resultList;
		}
		for (VideoEntry ve : videoEntries) {
			FetchDatum datum = new FetchDatum();
			Map<String, Object> metadata = new HashMap<String, Object>();
			String keyOfIsFetchType = "ift_"+feedName;
			metadata.put(keyOfIsFetchType, true);
			
			getId(ve, metadata);
			
			DateTime publishDate = ve.getPublished();
			getPublished(publishDate, metadata);
			
			DateTime updateDate = ve.getUpdated();
			getUpdated(updateDate, metadata);

			Set<Category> categories = ve.getCategories();
			getCategory(categories, metadata);

			String title = ve.getTitle().getPlainText();
			getTitle(title, metadata);
			
			String content = ve.getContent()==null?"":ve.getContent().getLang();
			getContent(content, metadata);

			List<Link> links = ve.getLinks();
			getLink(links, metadata);

			List<Person> authors = ve.getAuthors();
			getAuthor(authors, metadata);

			Comments comments = ve.getComments();
			getComments(comments, metadata);

			YouTubeMediaGroup mediaGroup = ve.getMediaGroup();
			getMediaGroup(mediaGroup, metadata);

			Rating rating = ve.getRating();
			getRating(rating, metadata);

			YtStatistics statistics = ve.getStatistics();
			getStatistics(statistics, metadata);

			datum.addAllMetadata(metadata);
			resultList.add(datum);
		}
		return resultList;
	}
	
	
	private void getUserStatistics(YtUserProfileStatistics statistics, Map<String, Object> metadata) {
		if(statistics!=null){
			Map<String, Object> statisticsMap = new HashMap<String, Object>();
			DateTime lastWebAccess_dt = statistics.getLastWebAccess();
			Date lastWebAccess = new Date(lastWebAccess_dt.getValue());
			statisticsMap.put("lastWebAccess", lastWebAccess);
			Long subscriberCount = statistics.getSubscriberCount();
			statisticsMap.put("subscriberCount", subscriberCount);
			long videoWatchCount = statistics.getVideoWatchCount();
			statisticsMap.put("videoWatchCount", videoWatchCount);
			Long viewCount = statistics.getViewCount();
			statisticsMap.put("viewCount", viewCount);
			Long totalUploadViews = statistics.getTotalUploadViews();
			statisticsMap.put("totalUploadViews",totalUploadViews);
			metadata.put("statistics", statisticsMap);
		}
	}

	private void getStatistics(YtStatistics statistics, Map<String, Object> metadata) {
		if(statistics!=null){
			Map<String, Object> statisticsMap = new HashMap<String, Object>();
			Long favoriteCount = statistics.getFavoriteCount();
			statisticsMap.put("favoriteCount", favoriteCount);
			Long viewCount = statistics.getViewCount();
			statisticsMap.put("viewCount", viewCount);
			metadata.put("statistics", statisticsMap);
		}
	}

	private void getRating(Rating rating, Map<String, Object> metadata) {
		if(rating!=null){
			Map<String, Object> ratingMap = new HashMap<String, Object>();
			float average = rating.getAverage();
			ratingMap.put("average", average);
			int max = rating.getMax();
			ratingMap.put("max", max);
			int min = rating.getMin();
			ratingMap.put("min", min);
			int numRaters = rating.getNumRaters();
			ratingMap.put("numRaters", numRaters);
			String rel_s = rating.getRel();
			ratingMap.put("rel", rel_s);
			metadata.put("rating", ratingMap);
		}
	}

	private void getMediaGroup(YouTubeMediaGroup mediaGroup, Map<String, Object> metadata) {
		if(mediaGroup!=null){
			Map<String, Object> mediaGroupMap = new HashMap<String, Object>();
			
			getMediaCategory(mediaGroup, mediaGroupMap);
			
			getMediaContent(mediaGroup, mediaGroupMap);
			
			getMediaDescription(mediaGroup, mediaGroupMap);
			
			getMediaKeywords(mediaGroup, mediaGroupMap);
			
			getMediaPlayer(mediaGroup, mediaGroupMap);
			
			getMediaThumbnail(mediaGroup, mediaGroupMap);
			
			getMediaTitle(mediaGroup, mediaGroupMap);
			
			getMediaDuration(mediaGroup, mediaGroupMap);
			
			metadata.put("mediaGroup", mediaGroupMap);
		}
	}

	private void getMediaDuration(YouTubeMediaGroup mediaGroup,
			Map<String, Object> mediaGroupMap) {
			Long mediaDuration = mediaGroup.getDuration();
			mediaGroupMap.put("mediaDuration", mediaDuration);
	}

	private void getMediaTitle(YouTubeMediaGroup mediaGroup,
			Map<String, Object> mediaGroupMap) {
		MediaTitle mediaTitle = mediaGroup.getTitle();
		if(mediaTitle!=null){
			String mediaTitle_s = mediaTitle.getPlainTextContent();
			mediaGroupMap.put("mediaTitle", mediaTitle_s);
		}
	}

	private void getMediaThumbnail(YouTubeMediaGroup mediaGroup,
			Map<String, Object> mediaGroupMap) {
		List<MediaThumbnail> mediaThumbnails = mediaGroup.getThumbnails();
		if(mediaThumbnails!=null){
			List<Map<String, Object>> mediaThumbnailList = new ArrayList<Map<String, Object>>();
			for (MediaThumbnail mediaThumbnail : mediaThumbnails) {
				Map<String, Object> mediaThumbnailMap = getThumbnailMap(mediaThumbnail);
				mediaThumbnailList.add(mediaThumbnailMap);
			}
			mediaGroupMap.put("mediaThumbnail", mediaThumbnailList);
		}
	}

	private Map<String, Object> getThumbnailMap(MediaThumbnail mediaThumbnail) {
		Map<String, Object> mediaThumbnailMap = new HashMap<String, Object>();
		if(mediaThumbnail!=null){
			String url1 = mediaThumbnail.getUrl();
			mediaThumbnailMap.put("url", url1);
			int height = mediaThumbnail.getHeight();
			mediaThumbnailMap.put("height", height);
			int width = mediaThumbnail.getWidth();
			mediaThumbnailMap.put("width", width);
			if(mediaThumbnail.getTime()!=null){
				String time =mediaThumbnail.getTime().toString();
				mediaThumbnailMap.put("time", time);
			}
		}
		return mediaThumbnailMap;
	}

	private void getMediaPlayer(YouTubeMediaGroup mediaGroup,
			Map<String, Object> mediaGroupMap) {
		MediaPlayer mediaPlayer = mediaGroup.getPlayer();
		Map<String, Object> mediaPlayerMap = new HashMap<String, Object>();
		if(mediaPlayer!=null){
			String url = mediaPlayer.getUrl();
			mediaPlayerMap.put("url", url);
			mediaGroupMap.put("mediaPlayer", mediaPlayerMap);
		}
	}

	private void getMediaKeywords(YouTubeMediaGroup mediaGroup,
			Map<String, Object> mediaGroupMap) {
		MediaKeywords mediaKeywords = mediaGroup.getKeywords();
		if(mediaKeywords!=null){
			List<String> mediaKeywordsList = mediaKeywords.getKeywords();
			mediaGroupMap.put("mediaKeywords", mediaKeywordsList);
		}
	}

	private void getMediaDescription(YouTubeMediaGroup mediaGroup,
			Map<String, Object> mediaGroupMap) {
		MediaDescription mediaDescription = mediaGroup.getDescription();
		if(mediaDescription!=null){
			String description = mediaDescription.getPlainTextContent();
			mediaGroupMap.put("mediaDescription", description);
		}
	}

	private void getMediaContent(YouTubeMediaGroup mediaGroup,
			Map<String, Object> mediaGroupMap) {
		List<MediaContent> mediaContents = mediaGroup.getContents();
		if(mediaContents!=null){
			List<Map<String, Object>> mediaContentList = new ArrayList<Map<String, Object>>();
			for (MediaContent mediaContent : mediaContents) {
				Map<String, Object> mediaContentMap = new HashMap<String, Object>();
				String url = mediaContent.getUrl();
				mediaContentMap.put("url", url);
				String type = mediaContent.getType();
				mediaContentMap.put("type", type);
				String medium = mediaContent.getMedium();
				mediaContentMap.put("medium", medium);
				boolean isDefault = mediaContent.isDefault();
				mediaContentMap.put("isDefault", isDefault);
				String expression = mediaContent.getExpression().toString();
				mediaContentMap.put("expression", expression);
				int duration = mediaContent.getDuration();
				mediaContentMap.put("duration", duration);
				int format = mediaContent.getFramerate();
				mediaContentMap.put("format", format);
				mediaContentList.add(mediaContentMap);
			}
			mediaGroupMap.put("mediaContent", mediaContentList);
		}
	}

	private void getMediaCategory(YouTubeMediaGroup mediaGroup,
			Map<String, Object> mediaGroupMap) {
		List<MediaCategory> mediaCategories = mediaGroup.getCategories();
		if(mediaCategories!=null){
			List<Map<String, Object>> mediaCategoryList = new ArrayList<Map<String, Object>>();
			for (MediaCategory mediaCategory : mediaCategories) {
				Map<String, Object> mediaCategoryMap = new HashMap<String, Object>();
				String label = mediaCategory.getLabel();
				mediaCategoryMap.put("label", label);
				String scheme = mediaCategory.getScheme();
				mediaCategoryMap.put("scheme", scheme);
				mediaCategoryList.add(mediaCategoryMap);
			}
			mediaGroupMap.put("mediaCategory", mediaCategoryList);
		}
	}

	private void getComments(Comments comments, Map<String, Object> metadata) {
		if(comments!=null){
			Map<String, Object> commentsMap = new HashMap<String, Object>();
			FeedLink<?> feedLink = comments.getFeedLink();
			getFeedLink(feedLink,commentsMap);
			metadata.put("comments", commentsMap);
		}
	}

	private void getFeedLink(FeedLink<?> feedLink,
			Map<String, Object> commentsMap) {
		if(feedLink!=null){
			Map<String, Object> feedlinkMap = new HashMap<String, Object>();
			String rel = feedLink.getRel();
			feedlinkMap.put("rel", rel);
			String href = feedLink.getHref();
			feedlinkMap.put("href", href);
			int countHint = feedLink.getCountHint();
			feedlinkMap.put("countHint", countHint);
			commentsMap.put("feedLink", feedlinkMap);
		}
	}
	
	
	private void getListFeedLink(List<FeedLink> feedLinks, Map<String, Object> metadata) {
		if(feedLinks!=null){
			List<Map<String, Object>> feedLinkList = new ArrayList<Map<String, Object>>();
			for (FeedLink<?> feedLink : feedLinks) {
				Map<String, Object> feedlinkMap = new HashMap<String, Object>();
				String rel = feedLink.getRel();
				feedlinkMap.put("rel", rel);
				String href = feedLink.getHref();
				feedlinkMap.put("href", href);
				Integer countHint = feedLink.getCountHint();
				feedlinkMap.put("countHint", countHint);
				feedLinkList.add(feedlinkMap);
				if("alternate".equals(rel)){
					metadata.put("viewurl", href);
				}
			}
			metadata.put("feedLink", feedLinkList);
		}
	}

	private void getAuthor(List<Person> authors, Map<String, Object> metadata) {
		if(authors!=null && authors.size()>0){
			List<Map<String, String>> authorList = new ArrayList<Map<String, String>>();
			for (Person person : authors) {
				Map<String, String> authorMap = new HashMap<String, String>();
				String name = person.getName();
				authorMap.put("name", name);
				String uri = person.getUri();
				authorMap.put("uri", uri);
				authorList.add(authorMap);
			}
			metadata.put("author", authorList);
		}
		
	}

	private void getLink(List<Link> links, Map<String, Object> metadata) {
		if(links!=null && links.size()>0){
			List<Map<String, String>> linkList = new ArrayList<Map<String, String>>();
			for (Link link : links) {
				Map<String, String> linkMap = new HashMap<String, String>();
				String rel = link.getRel();
				linkMap.put("rel", rel);
				String type = link.getType();
				linkMap.put("type", type);
				String href = link.getHref();
				linkMap.put("href", href);
				linkList.add(linkMap);
				if(rel.equals("alternate")){
					metadata.put("viewurl", href);
				}
			}
			metadata.put("link", linkList);
		}
	}

	private void getContent(String content, Map<String, Object> metadata) {
		metadata.put("content", content);
	}

	private void getTitle(String title, Map<String, Object> metadata) {
		metadata.put("title", title);
	}

	private void getCategory(Set<Category> categories, Map<String, Object> metadata) {
		if(categories!=null){
			Iterator<Category> it = categories.iterator();
			List<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();
			while (it.hasNext()) {
				Category category = it.next();
				Map<String, String> categoryMap = new HashMap<String, String>();
				String scheme = category.getScheme();
				categoryMap.put("scheme", scheme);
				String term = category.getTerm();
				categoryMap.put("term", term);
				String label = category.getLabel();
				categoryMap.put("label", label);
				categoryList.add(categoryMap);
			}
			metadata.put("category", categoryList);
		}
	}

	private void getUpdated(DateTime updateDate, Map<String, Object> metadata) {
		if(updateDate!=null){
			Date updated = new Date(updateDate.getValue());
			metadata.put("updated", updated);
		}
	}

	private void getPublished(DateTime publishDate, Map<String, Object> metadata) {
		if(publishDate!=null){
			Date published = new Date(publishDate.getValue());
			metadata.put("published", published);
		}
	}

	private void getId(VideoEntry ve, Map<String, Object> metadata) {
		if(ve!=null){
			String id = ve.getId();
			if(StringUtils.isNotEmpty(id)){
				String[] ids = id.split(":");
				id = ids[ids.length-1];
				metadata.put("id", id);
				metadata.put(Constants.OBJECT_ID, UUIDUtils.getMd5UUID(id));
			}
		}
		
	}
	
	private void getUserObjectId(String userName, Map<String, Object> metadata) {
		if(StringUtils.isNotEmpty(userName)){
			metadata.put(Constants.OBJECT_ID, UUIDUtils.getMd5UUID(userName));
		}
	}

}
