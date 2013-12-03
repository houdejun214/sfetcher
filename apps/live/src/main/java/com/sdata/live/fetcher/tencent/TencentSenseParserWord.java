package com.sdata.live.fetcher.tencent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;
import com.tencent.weibo.api.SearchAPI;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.constants.OAuthConstants;
import com.tencent.weibo.oauthv2.OAuthV2;

/**
 * @author zhufb
 *
 */
public class TencentSenseParserWord extends TencentSenseParser {
	private SearchAPI searchAPI;
	private UserAPI userAPI;
	//搜索类型
	//0-默认搜索类型（现在为模糊搜索） 
	//1-模糊搜索：时间参数starttime和endtime间隔小于一小时，时间参数会调整为starttime前endtime后的整点，即调整间隔为1小时 
	//8-实时搜索：选择实时搜索，只返回最近15分钟的微博，时间参数需要设置为最近的15分钟范围内才生效，并且不会调整参数间隔
	private String searchtype = "1";
	private String province="";//省编码（不填表示忽略地点搜索）
	private String city="";// 市编码（不填表示按省搜索）
	private String longitue="";//经度，（实数）*1000000，需与latitude、radius配合使用
	private String latitude="";// 纬度，（实数）*1000000，需与longitude、radius配合使用
	private String radius="";//半径（整数，单位米，不大于20000）,需与longitude、latitude配合使用
	private TencentJsonParser parser;
	private boolean complete = false;

	public TencentSenseParserWord(Configuration conf,OAuthV2 oauth){
		super(conf,oauth);
		this.searchAPI = new SearchAPI(OAuthConstants.OAUTH_VERSION_2_A);
		this.userAPI = new UserAPI(OAuthConstants.OAUTH_VERSION_2_A);
		this.parser = new TencentJsonParser();
	}
	
	@Override
	public List<FetchDatum> getList(SenseCrawlItem item,TencentCrawlState state) {
		this.complete = false;
		List<FetchDatum> tweetsList = new ArrayList<FetchDatum>();
		String keyword =  StringUtils.valueOf(item.getParam(CrawlItemEnum.KEYWORD.getName()));
		String starttime = this.getUnixTime(state.getStart());
		String endtime = this.getUnixTime(state.getEnd());
		try {
			super.await(1000);
			String  content = searchAPI.t(oauth, format, keyword, pagesize, String.valueOf(state.getPage()), contenttype, sorttype, msgtype, searchtype, starttime, endtime, province, city, longitue, latitude, radius);
			if(StringUtils.isEmpty(content)){
				complete = true;
				return tweetsList;
			}
			ParseResult parseResult = parser.parseTencentAPITweets(item,new RawContent(content));
			String errcode =StringUtils.valueOf( parseResult.getMetadata().get("errcode"));
			if(!errcode.equals("0")){
				String msg = StringUtils.valueOf( parseResult.getMetadata().get("msg"));
				log.error("*********** tencent tweets fetchDatumList() has error:【"+ msg +"】***********");
				complete = true;
				return tweetsList;
			}
			if(parseResult.isListEmpty()){
				complete = true;
				return tweetsList;
			}
			tweetsList.addAll(parseResult.getFetchList()); 
			String hasnext = StringUtils.valueOf( parseResult.getMetadata().get("hasnext"));
			if (hasnext.equals("0") || hasnext.equals("2")) {// "1" mean yes
				complete = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			complete =  true;
		}
		return tweetsList;
	}
	
	public Map<String, Object> getTencentUserInfo(String name) {
		Map<String, Object> userInfo = this.fetchUserInfo(name, userAPI, oauth,parser);
		return userInfo;
	}
    
	@Override
	public boolean isComplete() {
		return complete;
	}

	@Override
	public void next(TencentCrawlState state) {
		state.setPage(state.getPage()+1);
	}

	private boolean checkHaveUser(String userId) {
		return client.exists("tencent_users", userId);
	}
	
	@Override
	public SenseFetchDatum getDatum(SenseFetchDatum datum) {
		Map<String, Object> metadata = datum.getMetadata();
		if (metadata != null) {
			String name = StringUtils.valueOf(metadata.get("name"));
			boolean isHaveUser = this.checkHaveUser(name);
			if (!isHaveUser) {
				Map<String, Object> userInfo = getTencentUserInfo(name);
				if (userInfo != null) {
					userInfo.put(Constants.USER_ID, name);
					metadata.put(Constants.TENCENT_USER_INFO, userInfo);
				}
			}
		}
		return datum;
	}

	@Override
	public ParseResult parseCrawlItem(Configuration conf, RawContent rc,
			SenseCrawlItem item) {
		// TODO Auto-generated method stub
		return null;
	}
}
