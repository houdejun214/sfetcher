package com.sdata.hot.weibo;

import java.util.Calendar;
import java.util.Date;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.hot.store.HBaseClientFactoryBean;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class WeiboHotFetcher extends SdataFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.WeiboHotFetcher");
	private String HOST = "http://hot.weibo.com/ajax/feed?type=d&v=9999&date=%s&page=%d"; 
	private String start;
	private String end;
	private String retRelationTable;
	public WeiboHotFetcher(Configuration conf, RunState state) {
		super.setConf(conf);
		this.parser = new WeiboHotParser(conf);
		this.start = conf.get("hot.start", "20130901");
		this.end = conf.get("hot.end", "20131001");
		this.retRelationTable = conf.get("hbase.retRelation.table","rets");
	}
	
	@Override
	public void fetchDatumList(FetchDispatch dispatch) {
		Date std = DateTimeUtils.parse(start, "yyyyMMdd");
		Date etd = DateTimeUtils.parse(end, "yyyyMMdd");
		Date cur = std;
		while(cur.before(etd)||cur.equals(etd)){
			String curDstr = DateTimeUtils.format(cur, "yyyyMMdd");
			
			for(int i=1;i<=10;i++){
				try{
					String url = String.format(HOST, curDstr,i);
					RawContent r = HotUtils.getRawContent(url);
					if(r.getContent()==null||!r.getContent().startsWith("{")){
						break;
					}
					JSONObject json = JSONObject.fromObject(r.getContent());
					if(json==null||!json.containsKey("data")){
						break;
					}
					String mids = json.getJSONObject("data").getString("mids");
					if(StringUtils.isEmpty(mids)){
						break;
					}
					String[] ids = mids.split(",");
					for(String id:ids){
						boolean exists = HBaseClientFactoryBean.getObject(super.getConf()).exists(retRelationTable, Long.valueOf(id));
						if(exists){
							continue;
						}
						JSONObject curTweet = WeiboAPI.getInstance().fetchOneTweet(id);
						if(curTweet == null){
							continue;
						}
						// Get curTweet's retweets in 5 minutes
						((WeiboHotParser)parser).parseList(dispatch,curTweet);
					}
				}catch(Exception e){
					e.printStackTrace();
				}
				log.warn("date:"+curDstr+",page:"+i+",fetch end!!!");
			}	
			cur = DateTimeUtils.add(cur, Calendar.DATE, 1);
		}
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum){
		return datum;
	}
	
	public boolean isComplete(){
		return true;
	}
}
