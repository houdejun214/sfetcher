package com.sdata.live.fetcher.tencent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.ParseResult;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;
import com.tencent.weibo.api.StatusesAPI;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.beans.OAuth;
import com.tencent.weibo.constants.OAuthConstants;

public class TencentSenseParserUser extends TencentSenseParser {

	private StatusesAPI statusesAPI;
	private UserAPI userAPI;
	private String pagesize = "30"; 
	private TencentJsonParser parser;
	private boolean complete;
	
	public TencentSenseParserUser(Configuration conf,OAuth oauth){
		super(conf,oauth);
		this.statusesAPI = new StatusesAPI(OAuthConstants.OAUTH_VERSION_1);
		this.userAPI = new UserAPI(OAuthConstants.OAUTH_VERSION_1);
		this.oauth = oauth;
		this.parser = new TencentJsonParser();
		complete = false;
	}
	
	@Override
	public List<FetchDatum> getList(SenseCrawlItem item,TencentCrawlState state) {
		this.complete = false;
		List<FetchDatum> tweetsList = new ArrayList<FetchDatum>();
		String lastid = "0";
		String pagetime = "";
		String pageflag = "0";
		if(!StringUtils.isEmpty(state.getLastId())&&!StringUtils.isEmpty(state.getPageTime())){
			lastid = state.getLastId();
			pagetime = state.getPageTime();
			pageflag = "1";
		}
		
		try {
			String name =  StringUtils.valueOf(item.getParam(CrawlItemEnum.ACCOUNT.getName()));
			String  content = statusesAPI.userTimeline(oauth, format, pageflag, pagetime, pagesize, lastid, name, "0", "0", "0");
			if(StringUtils.isEmpty(content)){
				complete = true;
				return tweetsList;
			}
			ParseResult parseResult = parser.parseTencentAPITweets(item,new RawContent(content));
			Map metadata = parseResult.getMetadata();
			String errcode =StringUtils.valueOf(metadata.get("errcode"));
			if(!errcode.equals("0")){
				String msg = StringUtils.valueOf( metadata.get("msg"));
				log.error("*********** tencent tweets fetchDatumList() has error:【"+ msg +"】");
				complete = true;
				return tweetsList;
			}
			if(parseResult.isListEmpty()){
				complete = true;
				return tweetsList;
			}
			tweetsList.addAll(parseResult.getFetchList());
			state.setLastId(StringUtils.valueOf(metadata.get("lastid")));
			state.setPageTime(StringUtils.valueOf(metadata.get("pagetime")));
			String hasnext = StringUtils.valueOf( metadata.get("hasnext"));
			if (hasnext.equals("0") || hasnext.equals("2")) {// "0" mean yes
				complete = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			complete = true;
		}
		return tweetsList;
	}

	public Map<String, Object> getTencentUserInfo(String name) {
		Map<String, Object> userInfo = this.fetchUserInfo(name, userAPI, oauth, parser);
		return userInfo;
	}

	@Override
	public SenseFetchDatum getDatum(SenseFetchDatum datum) {
		return datum;
	}
	
	@Override
	public boolean isComplete() {
		return this.complete;
	}

	@Override
	public void next(TencentCrawlState state) {
		//nothing to do
		
	}

	@Override
	public ParseResult parseCrawlItem(Configuration conf, RawContent rc,
			SenseCrawlItem item) {
		// TODO Auto-generated method stub
		return null;
	}

}
