package com.sdata.component.fetcher;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.QueryUrl;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.component.site.FlickrApi;
import com.sdata.core.site.PageQuery;
import com.sdata.core.util.WebPageDownloader;

public class FlickrSearchAPI{
	
	protected static int MAX_BLOCK_PHOTOS=4000;
	
	protected static final String START_TIME="StartTime";
	protected static final String END_TIME="EndTime";
	
	protected static final String TOTAL_COUNT_PATTERN = "<photos.*total=\"(\\d+)\">";
	
	protected final FlickrApi api = new FlickrApi();
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrSearchAPI");
	
	protected boolean curTimeIntervalOver=false;
	
	private String curQuery="";
	
	protected int curPageNo=1;
	
	protected Date startTime;
	
	protected Date endTime;

	protected long lastAdjustSecTimeSpan=0;
	
	protected long lastAdjustPhotoCount = 0;
	
	protected Date confStartTime;

	protected Date confEndTime;

	protected Pattern totalCountPatter = Pattern.compile(TOTAL_COUNT_PATTERN,Pattern.DOTALL);
	
	public FlickrSearchAPI(Date confStartTime,Date confEndTime){
		this.confStartTime = confStartTime;
		this.confEndTime = confEndTime;
		this.reset();
	}

	public String search(String curQuery){
		if(StringUtils.isNotEmpty(this.curQuery) && !this.curQuery.equals(curQuery)){
			this.reset();
		}
		this.curQuery = curQuery;
		PageQuery query = new PageQuery(curQuery,curPageNo,this.startTime,this.endTime);
		String queryUrl = api.getSearchQueryUrl(query);
		log.info("current query [{}]",query.toString());
		String content = WebPageDownloader.download(queryUrl);
		if(curPageNo<=1){
			//The first page query for a query , to split the query block to more small timespan.
			content = modifyCurrentQueryTimeBlock(queryUrl,content);
		}
		return content;
	}
	
	protected boolean moveNext() {
		if(curTimeIntervalOver){
			if(this.startTime!=null && this.startTime.after(confStartTime)){
				this.endTime = this.startTime;
				if(lastAdjustPhotoCount<=100){
					this.startTime = (Date)this.confStartTime.clone();
				}else{
					this.startTime = DateTimeUtils.add(endTime, Calendar.SECOND, -(int)(lastAdjustSecTimeSpan*(MAX_BLOCK_PHOTOS*1.0/lastAdjustPhotoCount)));
					if(this.startTime.before(this.confStartTime)){
						this.startTime = (Date) this.confStartTime.clone();
					}
				}
			}else{
				reset();
				// this query search is complete;
				return false;
			}
			curPageNo=1;
		}else{
			curPageNo++;
		}
		return true;
	}
	
	public String getState(){
		return curQuery+","+curPageNo+","+dateFormat(this.startTime)+"--"+ dateFormat(this.endTime);
	}
	
	public void setResultList( List<?> list ){
		if(list==null || Math.abs(list.size() - FlickrApi.PerPage)>=100 ){
			this.curTimeIntervalOver = true;
		}
	}
	
	private void reset(){
		this.curQuery = "";
		this.curPageNo=1;
		this.lastAdjustSecTimeSpan = 0;
		this.lastAdjustPhotoCount = 0;
		this.startTime = (Date)confStartTime.clone();
		this.endTime = (Date)confEndTime.clone();
	}
	
	protected String modifyCurrentQueryTimeBlock(String queryUrl, String content){
		long total = getTotalPhotosCount(content);
		if(total <= MAX_BLOCK_PHOTOS){
			return content;
		}
		//int timeskip = 24*60*60*5;  //5 days
		if(startTime==null || endTime==null){
			return content;
		}
		// cal the new block photos count
		QueryUrl query = UrlUtils.parseQueryUrlString(queryUrl);
		Date upper_bound = (Date)this.endTime.clone();
		Date lower_bound = getBetweenTime(this.startTime, this.endTime,(float) (MAX_BLOCK_PHOTOS*1.0/total));
		
		//Date lower_bound = (Date)this.startTime.clone();
		if(lower_bound.before(this.confStartTime)){
			lower_bound = (Date) this.confStartTime.clone();
		}
		int keep_going=8;
		while( keep_going > 0 && lower_bound.compareTo(this.confStartTime)>=0){
			query.setParameter("min_upload_date", api.dateToString(lower_bound));
			query.setParameter("max_upload_date", api.dateToString(upper_bound));
			queryUrl = query.toString();
			content = WebPageDownloader.download(queryUrl);//
			total = getTotalPhotosCount(content);
			if(total <= MAX_BLOCK_PHOTOS){
				break;
			}
			lower_bound = getBetweenTime(lower_bound ,upper_bound,(float) (MAX_BLOCK_PHOTOS*1.0/total) );//midpoint between current value and lower bound.
			keep_going--;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				;
			}
			
		}
		lastAdjustSecTimeSpan = (upper_bound.getTime() - lower_bound.getTime())/1000;
		lastAdjustPhotoCount = total;
		startTime = lower_bound;
		log.info("adjust the time blcok to "+getTimeSpan(this.startTime,this.endTime)+" "+queryUrl);
		return content;
	}
	
	protected long getTotalPhotosCount(String content) {
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
	
	protected Date getBetweenTime(Date start,Date end,float weight){
		long time = (long)(end.getTime()*(1-weight)+start.getTime()*weight);
		return new Date(time);
	}
	
	protected String dateFormat(Date time){
		return DateTimeUtils.format(time, "yyyy-MM-dd HH:mm:ss");
	}
	
	protected String getTimeSpan(Date time1,Date time2){
		return "["+DateTimeUtils.format(time1, "yyyy-MM-dd HH:mm:ss")+" - " + DateTimeUtils.format(time2, "yyyy-MM-dd HH:mm:ss")+ "]";
	}
	
	protected Date parseToDate(String str){
		return DateTimeUtils.parse(str, "yyyy-MM-dd HH:mm:ss");
	}
}
