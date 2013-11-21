package com.sdata.component.fetcher;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.QueryUrl;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.component.parser.FlickrParser;
import com.sdata.component.site.FlickrApi;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.site.PageQuery;
import com.sdata.core.util.ApplicationResourceUtils;
import com.sdata.core.util.WebPageDownloader;

public class FlickrFetcher extends SdataFetcher{
	
	private static final int MAX_BLOCK_PHOTOS=4000;
	
	private static final String START_TIME="StartTime";
	
	
	private static final String QUERY_INPUT_FILE="query.input.file";
	
	private static final String FLICKR_QUERY_DEFAULT_FILE="sites/query.flickr.list";
	
	private static final String TOTAL_COUNT_PATTERN = "<photos.*total=\"(\\d+)\">";
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrFetcher");
	
	private final FlickrApi api = new FlickrApi();
	
	private Queue<String> queries=null;
	
	private String curQuery=null;
	
	private boolean curQueryOver=false;
	
	private int curPageNo=1;
	
	private Date startTime;
	
	private Date endTime;

	private long lastAdjustSecTimeSpan=0;
	
	private long lastAdjustPhotoCount = 0;
	
	private final Date confStartTime;

	private final Date confEndTime;

	private final Pattern totalCountPatter;
	
	public FlickrFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		this.parser = new FlickrParser(conf,state);
		totalCountPatter = Pattern.compile(TOTAL_COUNT_PATTERN,Pattern.DOTALL);
		// init fetch state;
		//states is like as "keyword,2,2007-05-28 19:14:42--2007-05-29 10:21:29"
		String currentFetchState = this.state.getCurrentFetchState();
		if(StringUtils.isNotEmpty(currentFetchState)){
			// this mean the last task isn't finish normal,and now this new task will begin with the last task
			confStartTime = state.getCurrentTaskStartTime();
			confEndTime = state.getCurrentTaskEndTime();
			String[] states = currentFetchState.split(",", 3);
			if(states!=null && states.length >= 2){
				this.curQuery = states[0];
				this.curPageNo = Integer.valueOf(states[1]);
			}
			if(states.length==3){
				String _timeSpan = states[2];
				String[] times = _timeSpan.split("--");
				if(times.length==2){
					if(StringUtils.isNotEmpty(times[0])){
						this.startTime = parseToDate(times[0]);
					}
					if(StringUtils.isNotEmpty(times[1])){
						this.endTime = parseToDate(times[1]);
					}
				}
			}
		}else{
			Date lastTaskEndTime = state.getLastTaskEndTime();
			if(lastTaskEndTime==null){
				//this task is the first task
				confStartTime = getConfDate(START_TIME);
				
			}else{
				confStartTime = lastTaskEndTime;
			}
			confEndTime =new Date();
			startTime = (Date)confStartTime.clone();
			endTime = (Date)confEndTime.clone();
			
		}
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		if(queries==null){
			loadList();
		}
		
		if(StringUtils.isEmpty(curQuery) && queries.size()>0){
			curQuery = queries.poll();
		}
		if(!StringUtils.isEmpty(curQuery)){
			curQueryOver = false;
			PageQuery query = new PageQuery(curQuery,curPageNo,this.startTime,this.endTime);
			String queryUrl = api.getSearchQueryUrl(query);
			try {
				log.info("fetching page list ["+query.toString()+"] "+queryUrl);
				String content = WebPageDownloader.download(queryUrl);
				if(curPageNo<=1){
					//The first page query for a query , to split the query block to more small timespan.
					content = modifyCurrentQueryTimeBlock(queryUrl,content);
				}
				ParseResult parseList = parser.parseList(new RawContent(content));
				if(parseList.isListEmpty() || Math.abs(parseList.getListSize() - FlickrApi.PerPage)>=100 ){
					curQueryOver = true;
				}
				moveNext();
				// set the metadata information of current query keywords
				List<FetchDatum> fetchList = parseList.getFetchList();
				for(FetchDatum datum: fetchList){
					datum.addMetadata("query", curQuery);
					datum.setCurrent(curQuery+","+curPageNo+","+dateFormat(this.startTime)+"--"+ dateFormat(this.endTime));
				}
				return fetchList;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public String modifyCurrentQueryTimeBlock(String queryUrl, String content){
		long total = getTotalPhotosCount(content);
		if(total <= MAX_BLOCK_PHOTOS){
			return content;
		}
		//int timeskip = 24*60*60*5;  //5 days
		if(startTime==null || endTime==null){
			return content;
		}
		//if()
		// cal the new block photos count
		QueryUrl query = UrlUtils.parseQueryUrlString(queryUrl);
		Date lower_bound = (Date)this.startTime.clone();
		Date upper_bound = getBetweenTime(this.startTime, this.endTime,(float) (MAX_BLOCK_PHOTOS*1.0/total));
		if(upper_bound.after(this.confEndTime)){
			upper_bound = (Date) this.confEndTime.clone();
		}
		int keep_going=8;
		while( keep_going > 0 && upper_bound.compareTo(this.confEndTime)<=0){
			query.setParameter("min_upload_date", api.dateToString(lower_bound));
			query.setParameter("max_upload_date", api.dateToString(upper_bound));
			queryUrl = query.toString();
			content = WebPageDownloader.download(queryUrl);//
			total = getTotalPhotosCount(content);
			if(total <= MAX_BLOCK_PHOTOS){
				break;
			}
			upper_bound = getBetweenTime(lower_bound ,upper_bound,(float) (MAX_BLOCK_PHOTOS*1.0/total) );//midpoint between current value and lower bound.
			keep_going--;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				;
			}
			
		}
		lastAdjustSecTimeSpan = (upper_bound.getTime() - lower_bound.getTime())/1000;
		lastAdjustPhotoCount = total;
		endTime = upper_bound;
		log.info("adjust the time blcok to "+getTimeSpan(this.startTime,this.endTime)+" "+queryUrl);
		return content;
	}
	
	
	private long getTotalPhotosCount(String content) {
		if(StringUtils.isEmpty(content)){
			return 0;
		}
		int total = 0;
		Matcher matcher = totalCountPatter.matcher(content);
		if(matcher.find()){
			String strTotal = matcher.group(1);
			total = Integer.parseInt(strTotal);
		}
		return total;
	}
	
	private Date getBetweenTime(Date start,Date end,float weight){
		long time = (long)(end.getTime()*weight+start.getTime()*(1-weight));
		return new Date(time);
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		if(datum!=null && !StringUtils.isEmpty( datum.getUrl())){
			String url = datum.getUrl();
			log.debug("fetching "+url);
			String content = WebPageDownloader.download(url);
			//datum.setContent(content);
			RawContent rawContent = new RawContent(url,content);
			ParseResult result = parser.parseSingle(rawContent);
			if(result != null){
				datum.setMetadata(result.getMetadata());
				datum.addMetadata(Constants.FLICKR_IS_COMMENTS_INDEPENDENCE, true);
			}else{
				throw new RuntimeException("fetch content is empty");
			}
		}
		return datum;
	}
	
	/**
	 * move to next crawl instance
	 */
	@Override
	protected void moveNext() {
		if(curQueryOver){
			if(this.endTime!=null && this.endTime.before(confEndTime)){
				this.startTime = this.endTime;
				if(lastAdjustPhotoCount<=100){
					this.endTime = (Date)this.confEndTime.clone();
				}else{
					this.endTime = DateTimeUtils.add(startTime, Calendar.SECOND, (int)(lastAdjustSecTimeSpan*(MAX_BLOCK_PHOTOS*1.0/lastAdjustPhotoCount)));
					if(this.endTime.after(this.confEndTime)){
						this.endTime = (Date) this.confEndTime.clone();
					}
				}
			}else{
				curQuery = queries.poll();
				this.startTime = (Date)confStartTime.clone();
				this.endTime = (Date)confEndTime.clone();
			}
			curPageNo = 1;
		}else{
			curPageNo++;
		}
	}
	
	@Override
	public boolean isComplete(){
		boolean isComplete = StringUtils.isEmpty(this.curQuery) && this.queries.size()<=0;
		if(isComplete){
			//update currentFetchState to a empty String for next task
			state.updateCurrentFetchState("");
		}
		return isComplete;
	}
	
	private void loadList() {
		String file = this.getConf(QUERY_INPUT_FILE,FLICKR_QUERY_DEFAULT_FILE);
		file = ApplicationResourceUtils.getResourceUrl(file);
		try {
			List<String> readLines = FileUtils.readLines(new File(file), "utf-8");
			if(!StringUtils.isEmpty(curQuery)){
				int index = readLines.indexOf(curQuery);
				if(index>-1){
					readLines = readLines.subList(index+1, readLines.size());
				}
			}
			queries = new LinkedList<String>(readLines);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	private String dateFormat(Date time){
		return DateTimeUtils.format(time, "yyyy-MM-dd HH:mm:ss");
	}
	
	private String getTimeSpan(Date time1,Date time2){
		return "["+DateTimeUtils.format(time1, "yyyy-MM-dd HH:mm:ss")+" - " + DateTimeUtils.format(time2, "yyyy-MM-dd HH:mm:ss")+ "]";
	}
	
	private Date parseToDate(String str){
		return DateTimeUtils.parse(str, "yyyy-MM-dd HH:mm:ss");
	}
}
