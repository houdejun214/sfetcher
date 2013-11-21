package com.sdata.component.word.tencent;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.nus.next.db.hbase.HBaseDao;
import com.nus.next.db.hbase.HBaseFactory;
import com.nus.next.db.hbase.thrift.HBaseClient;
import com.nus.next.db.hbase.thrift.HBaseClientFactory;
import com.sdata.component.word.WordParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.item.CrawlItem;
import com.sdata.core.parser.ParseResult;
import com.tencent.weibo.api.SearchAPI;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.constants.OAuthConstants;
import com.tencent.weibo.oauthv1.OAuthV1;

public class TencentWordParser extends WordParser {

	protected static final Logger log = LoggerFactory
			.getLogger("SdataCrawler.TencentWordParser");
	private SearchAPI searchAPI;
	private UserAPI userAPI;
	private OAuthV1 oauth;

	protected String format = "json";// 返回数据的格式（json或xml）
	protected String pagesize = "30";// 每页大小（1-30个）
	protected String contenttype = "0";// 消息的正文类型（按位使用）0-所有，0x01-纯文本，0x02-包含url，0x04-包含图片，0x08-包含视频，0x10-包含音频
	protected String sorttype = "0";// 排序方式 0-表示按默认方式排序(即时间排序(最新))
	protected String msgtype = "0";// 消息的类型（按位使用）0-所有，1-原创发表，2
									// 转载，8-回复(针对一个消息，进行对话)，0x10-空回(点击客人页，进行对话)
	// 搜索类型
	// 0-默认搜索类型（现在为模糊搜索）
	// 1-模糊搜索：时间参数starttime和endtime间隔小于一小时，时间参数会调整为starttime前endtime后的整点，即调整间隔为1小时
	// 8-实时搜索：选择实时搜索，只返回最近几分钟的微博，时间参数需要设置为最近的几分钟范围内才生效，并且不会调整参数间隔
	private String searchtype = "0";
	private String starttime = "";// 开始时间，用UNIX时间表示（从1970年1月1日0时0分0秒起至现在的总秒数）
	private String endtime = "";// 结束时间，与starttime一起使用（必须大于starttime）
	private String province = "";// 省编码（不填表示忽略地点搜索）
	private String city = "";// 市编码（不填表示按省搜索）
	private String longitue = "";// 经度，（实数）*1000000，需与latitude、radius配合使用
	private String latitude = "";// 纬度，（实数）*1000000，需与longitude、radius配合使用
	private String radius = "";// 半径（整数，单位米，不大于20000）,需与longitude、latitude配合使用
	private boolean complete = false;
	private int page;
//	HBaseDao hBaseDao;
	HBaseClient client;

	private TencentJsonParser parser;

	public TencentWordParser(Configuration conf) {
		String consumerKey = conf.get("ConsumerKey");
		String consumerSecret = conf.get("ConsumerSecret");
		String AccessToken = conf.get("AccessToken");
		String AccessTokenSecret = conf.get("AccessTokenSecret");
		OAuthV1 oauth = new OAuthV1();
		oauth.setOauthConsumerKey(consumerKey);
		oauth.setOauthConsumerSecret(consumerSecret);
		oauth.setOauthToken(AccessToken);
		oauth.setOauthTokenSecret(AccessTokenSecret);
		searchAPI = new SearchAPI(OAuthConstants.OAUTH_VERSION_1);
		userAPI = new UserAPI(OAuthConstants.OAUTH_VERSION_1);
		this.oauth = oauth;
		parser = new TencentJsonParser();
		client = HBaseClientFactory.getClientWithNormalSeri("next");
	}

	public long getUnixTime(Date date) {
		Calendar instance = Calendar.getInstance();
		instance.setTime(date);
		return instance.getTimeInMillis() / 1000;
	}

	@Override
	public List<FetchDatum> getFetchList(CrawlItem item, Date startTime,
			Date endTime, String currentState) {
		complete = false;
		if(StringUtils.isEmpty(currentState)){
			page = 1;
		}else{
			page = Integer.valueOf(currentState);
		}
		
		List<FetchDatum> tweetsList = new ArrayList<FetchDatum>();
		this.starttime = String.valueOf(getUnixTime(startTime));
		this.endtime = String.valueOf(getUnixTime(endTime));
		while (true) {
			String content;
			try {
				content = searchAPI.t(oauth, format, item.getKeyword(),
						pagesize, StringUtils.valueOf(page), contenttype, sorttype,
						msgtype, searchtype, starttime, endtime, province,
						city, longitue, latitude, radius);
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
			if (content == null || content.equals("")) {
				continue;
			} else {
				// this.await(5000);
				ParseResult parseResult = parser
						.parseTencentAPITweets(new RawContent(content),item);
				String errcode = StringUtils.valueOf(parseResult.getMetadata()
						.get("errcode"));
				if (!errcode.equals("0")) {
					String msg = StringUtils.valueOf(parseResult.getMetadata()
							.get("msg"));
					log.error("*********** tencent tweets fetchDatumList() has error:【"
							+ msg + "】***********");
					if ("access rate limit".equals(msg)) {
						this.await(60000);
						continue;
					} else {
						complete = true;
						return tweetsList;
					}
				}
				String hasnext = StringUtils.valueOf(parseResult.getMetadata()
						.get("hasnext"));
				if (parseResult.isListEmpty()) {
					complete = true;
					return tweetsList;
				}
				tweetsList.addAll(parseResult.getFetchList());
				if (hasnext.equals("0") || hasnext.equals("2")) {// "0" mean yes
					complete = true;
					break;
				}
			}
			break;
		}
		if(++page>50){
			complete = true;
		}
		return tweetsList;
	}

	protected Map<String, Object> fetchUserInfo(String name, UserAPI userAPI,
			OAuthV1 oauth, TencentJsonParser parser) {
		String userJson = null;
		while (true) {
			try {
				userJson = userAPI.otherInfo(oauth, "json", name, null);
				if (StringUtils.isEmpty(userJson)) {
					continue;
				} else {
					ParseResult parseResult = parser
							.parseUserInfo(new RawContent(userJson));
					Map metadata = parseResult.getMetadata();
					Map userInfoMap = (Map) metadata
							.get(Constants.TENCENT_USER);
					String errcode = StringUtils.valueOf(userInfoMap
							.get("rerrcode"));
					if (!errcode.equals("0")) {
						String msg = StringUtils.valueOf(userInfoMap
								.get("rmsg"));
						log.error("*********** tencent user 【" + name
								+ "】 has error:【" + msg + "】");
						return null;
					}
					userInfoMap.remove(Constants.TENCENT_USER_TWEETINFO);
					return (Map<String, Object>) userInfoMap;
				}
			} catch (Exception e) {
				continue;
			}

		}
	}


	public Map<String, Object> getTencentUserInfo(String name) {
		Map<String, Object> userInfo = this.fetchUserInfo(name, userAPI, oauth,
				parser);
		return userInfo;
	}

	@Override
	public boolean isComplete() {
		return complete;
	}

	private boolean checkHaveUser(String userId) {
		return client.exists("word_tencent_users", userId);
	}

	@Override
	public FetchDatum getFetchDatum(FetchDatum datum) {
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
	public String getCurrentState() {
		return StringUtils.valueOf(page);
	}

}
