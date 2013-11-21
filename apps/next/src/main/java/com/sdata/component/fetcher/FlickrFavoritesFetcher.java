package com.sdata.component.fetcher;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.ImageMgDao;
import com.sdata.component.data.dao.LeafUserMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.component.parser.FlickrFavoritesParser;
import com.sdata.component.site.FlickrApi;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.util.WebPageDownloader;

/**
 * fetch user relationship of Tencent weibo
 * 
 * the fetcher use multiple-threads to fetch list of a user' relationship 
 * 
 * @author qiumm
 *
 */
public class FlickrFavoritesFetcher extends FlickrBaseFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrFavoritesFetcher");
	
	private Boolean isComplete = false;
	
	private int maxFavoritesNum;
	private static final int getImagesNum = 100;
	
	private final FlickrApi flickrAPI;
	
	private UserMgDao userdao = new UserMgDao();
	private LeafUserMgDao leafuserdao = new LeafUserMgDao();
	private ImageMgDao imagedao = null;
	
	private String currentFetchState;

	public FlickrFavoritesFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		this.parser = new FlickrFavoritesParser(conf,state);
		flickrAPI = new FlickrApi();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.userdao.initilize(host, port, dbName);
		this.leafuserdao.initilize(host, port, dbName);
		imagedao = new ImageMgDao(conf.get(Constants.SOURCE));
		currentFetchState = state.getCurrentFetchState();
		maxFavoritesNum = this.getConfInt("maxFavoritesNum", 1000);
	}
	
	/**
	 * this method will be call with multiple-thread
	 * @throws  
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		int skipNum;
		if(StringUtils.isEmpty(currentFetchState)){
			//get photoes from mongodb from start
			skipNum = 0;
		}else{
			//get photoes from mongodb from currentFetchState
			skipNum = Integer.valueOf(currentFetchState);
		}
		List<Map<String,Object>> imagesList = imagedao.getNextFetchList(skipNum, getImagesNum);
		if(imagesList.size()<=0){
			this.await(3600000);
		}else{
			currentFetchState =StringUtils.valueOf(skipNum+imagesList.size());
			for(int i=0;i<imagesList.size();i++){
				FetchDatum datum = new FetchDatum();
				Map<String,Object> imageMap = imagesList.get(i);
				String orgid =StringUtils.valueOf(imageMap.get("orgid"));
				datum.setId(orgid);
				datum.addAllMetadata(imageMap);
				if(i==imagesList.size()-1){
					datum.setCurrent(currentFetchState);
				}
				resultList.add(datum);
			}
		}
		return resultList;
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		Map<String, Object> metadata = datum.getMetadata();
		String photoId = String.valueOf(datum.getId());
		List<Map<String,Object>> favoritesList = this.getFavoritesByPhoto(photoId);
		for(Map<String,Object> fMap:favoritesList){
			Long uid = (Long)fMap.get("uid");
			boolean inUser = userdao.isExists(uid);
			fMap.put("inUser", inUser);
//			boolean inLeafUser = leafuserdao.isExists(uid);
//			fMap.put("inLeafUser", inLeafUser);
		}
		log.info("success fetch photo["+photoId+"]'s favorites list size["+favoritesList.size()+"].");
		metadata.put(Constants.FLICKR_FAVORITES, favoritesList);
		metadata.put("totalFavNum", favoritesList.size());
		return datum;
	}
	
	

	private List<Map<String, Object>> getFavoritesByPhoto(String photoId) {
		List<Map<String, Object>> favoritesList =new ArrayList<Map<String, Object>>();
		int page=1;
		while(true){
			String queryUrl = null;
			String content = null;
			int exceptionNum = 0;
			try {
				queryUrl = flickrAPI.getPhotosFavoritesUrl(photoId, page);
				content = WebPageDownloader.download(queryUrl);
			} catch (Exception e) {
				log.info("we get Exception when fetch photo["+photoId+"] favorites list ,url is: "+queryUrl);
				exceptionNum++;
				if(exceptionNum>10){
					log.error(e.getMessage());
					break;
				}
			}
			RawContent rawContent = new RawContent(content);
			ParseResult result = parser.parseSingle(rawContent);
			if(result == null){
				throw new NegligibleException("fetch content is empty");
			}
			Map<?, ?> metadata = result.getMetadata();
			String stat = StringUtils.valueOf(metadata.get("stat"));
			String message = StringUtils.valueOf(metadata.get("message"));
			if(!stat.equals("ok")){
				log.warn("fetch photo["+photoId+"]'s favorites list stat ["+stat+"]:"+message);
				break;
			}
			String totalPages = StringUtils.valueOf(metadata.get("pages"));
			if(totalPages.equals("0")){
//				log.info("photo["+photoId+"] do't has any favorite " );
				break;
			}
			List<Map<String,Object>> fList = (List<Map<String,Object>>)metadata.get(Constants.FLICKR_FAVORITES);
			favoritesList.addAll(fList);
			int curPage =Integer.valueOf(StringUtils.valueOf(metadata.get("page"))) ;
			int tPage = Integer.valueOf(totalPages);
			if(tPage<=curPage){//means this page is the last page
				break;
			}
			if(favoritesList.size()>maxFavoritesNum){
				break;
			}
			page++;
		}
		return favoritesList;
	}

	
	/**
	 * move to next crawl instance
	 * @param curUser 
	 */
	protected void moveNext() {
		
	}
	
	
	@Override
	public void datumFinish(FetchDatum datum) {
		if(datum!=null && StringUtils.isNotEmpty(datum.getCurrent())){
			state.updateCurrentFetchState(datum.getCurrent());
		}
	}

	@Override
	public boolean isComplete(){
		return isComplete;
	}
	
}
