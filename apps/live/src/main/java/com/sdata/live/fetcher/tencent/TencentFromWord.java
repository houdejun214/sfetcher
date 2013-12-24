package com.sdata.live.fetcher.tencent;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Document;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.core.util.JsoupUtils;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;
import com.tencent.weibo.api.TAPI;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.beans.OAuth;
import com.tencent.weibo.constants.OAuthConstants;

/**
 * @author zhufb
 *
 */
public class TencentFromWord extends TencentBase {
	
	private TAPI tAPI;
	private UserAPI userAPI;
	private TencentJsonParser parser;
	private boolean complete = false;
	//SearchUrl
    private String searchUrl = "http://search.t.qq.com/index.php?k={0}&s_time={1}%2C{2}&s_advanced=1&s_m_type=1&p={3}";

	public TencentFromWord(Configuration conf,OAuth oauth){
		super(conf,oauth);
		this.tAPI = new TAPI(OAuthConstants.OAUTH_VERSION_1);
		this.userAPI = new UserAPI(OAuthConstants.OAUTH_VERSION_1);
		this.parser = new TencentJsonParser();
	}
	
	@Override
	public List<FetchDatum> getList(SenseCrawlItem item,TencentState state) {
		this.complete = false;
		List<FetchDatum> tweetsList = new ArrayList<FetchDatum>();
		String keyword =  StringUtils.valueOf(item.getParam(CrawlItemEnum.KEYWORD.getName()));
		String starttime =DateTimeUtils.format(state.getStart(), "yyyyMMddHHmmss");// this.getUnixTime(state.getStart());
		String endtime = DateTimeUtils.format(state.getEnd(), "yyyyMMddHHmmss");//this.getUnixTime(state.getEnd());
		try {
			String url = MessageFormat.format(searchUrl,keyword,starttime,endtime,state.getPage());
			Document document = null;
			while(true){
				document = DocumentUtils.getDocument(url, header);
				if(isValid(document)){
					break;
				}
				super.refreshHeader();
			}
			if(document == null||!StringUtils.isEmpty(document.select(".noresult").text())){
				complete = true;
				return tweetsList;
			}
			List<String> ids = JsoupUtils.getListAttr(document, "#talkList li", "id");
			if(ids == null||ids.isEmpty()){
				complete = true;
				return tweetsList;
			}
			StringBuffer sb = new StringBuffer();
			for(String id:ids){
				sb.append(",").append(id);
			}
			String content = tAPI.showList(super.oauth,super.format,sb.substring(1));
			if(StringUtils.isEmpty(content)){
				complete = true;
				return tweetsList;
			}
			
			ParseResult parseResult = parser.parseTencentAPITweets(item,new RawContent(content));
			if(parseResult.isListEmpty()){
				complete = true;
				return tweetsList;
			}
			tweetsList.addAll(parseResult.getFetchList()); 
			String link = JsoupUtils.getLink(document, ".pageNav .pageBtn:contains(下一页)");
			if(StringUtils.isEmpty(link)){
				complete = true;
				return tweetsList;
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
	public void next(TencentState state) {
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
}
