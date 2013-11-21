package com.sdata.component.parser;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import weibo4j.Weibo;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlUtils;
import com.mongodb.DBObject;
import com.sdata.component.data.dao.FamousMgDao;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.component.site.WeiboTweetAPI;
import com.sdata.component.site.WeiboUserAPI;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.http.HttpPage;
import com.sdata.core.http.HttpPageLoader;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;


/**
 * @author zhufb
 *
 */
public class WeiboFamousParser extends SdataParser {

	TweetsMgDao dao = new TweetsMgDao();
	private static final String starHost = "http://data.weibo.com/top/influence/famous?";
	private static final String starTweetsUrl = "http://s.weibo.com/weibo/%2527%2527&xsort=time&userscope=custom:%s&page=%d";
	private static final Log log = LogFactory
			.getLog("SdataCrawler.WeiboFamousParser");
	private WeiboUserAPI userFetcher;
	private WeiboTweetAPI tweetFetcher;
	private FamousMgDao famousMgDao;
	private TweetsMgDao tweetsMgDao;
	private int maxPage = 2;
	private int htmlWait = 25;
	private int apiWait = 25;

	public WeiboFamousParser(Configuration conf, RunState state) {
		setConf(conf);
		setRunState(state);
		userFetcher = new WeiboUserAPI(conf, state);
		tweetFetcher = new WeiboTweetAPI(conf, state);
		this.famousMgDao = new FamousMgDao();
		this.tweetsMgDao = new TweetsMgDao();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort", 27017);
		String dbName = this.getConf("mongoDbName");
		this.famousMgDao.initilize(host, port, dbName);
		this.tweetsMgDao.initilize(host, port, dbName);
	}

	/**
	 * fetch topic list from html content
	 * 
	 * @param content
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public JSONArray parseFamousList(String content) {
		Document doc = parseHtmlDocument(content);
		if (doc == null) {
			return null;
		}
		JSONArray result = new JSONArray();
		Elements pages = doc
				.select("[node-type=page] a:not(.current,.W_btn_a)");
		for (int i = 0; i <= pages.size(); i++) {
			Elements stars = doc.select(".box_Show_z tr");
			for (int s = 1; s < stars.size(); s++) {
				Element star = stars.get(s);
				int order = 50 * i
						+ Integer.parseInt(star.select(".num").first().text());
				String popular = star.select(".times_zw").first().text();
				String home = star.select(".zw_name a").first().attr("href");
				String name = star.select(".zw_name a").first().text();
				Map<String, Object> famous = new HashMap<String, Object>();
				famous.put(Constants.FAMOUS_ORDER, order);
				famous.put(Constants.FAMOUS_POPULAR, Integer.parseInt(popular));
				famous.put(Constants.FAMOUS_HOMEPAGE, home);
				famous.put(Constants.FAMOUS_NAME, name);
				result.add(famous);
			}
			if (i < pages.size()) {
				Element page = pages.get(i);
				doc = this.fetchPage(starHost + page.attr("action-data"));
			}
		}
		return result;
	}

	/**
	 * fetch topic's topic's description and tweets list
	 * 
	 * @param datum
	 */
	@SuppressWarnings("unchecked")
	public void packageFamousInfo(FetchDatum datum) {
		// fetch star
		String name = datum.getName();
		Map<String, Object> famous = datum.getMetadata();
		try {
			Long sinceId = 0l;
			DBObject dbFamous = famousMgDao.queryByName(name);
			if (dbFamous == null) {
				JSONObject user = new JSONObject();
				try {
					user = userFetcher.fetchUserByName(name);
				} catch (Exception e) {
					log.warn("fetch one famous error,name:" + name
							+ ",can't catch it's user info!");
					e.printStackTrace();
					return;
				}
				Long uid = user.getLong("id");
				datum.setId(String.valueOf(uid));
				// delete user's best new tweet
				user.remove("status");

				// fetch famous's info
				famous.put(Constants.FAMOUS_ID, uid);
				famous.put(Constants.FAMOUS_ACCOUNT, user);
				famous.put(Constants.USER_DESCRIPTION,
						user.get(Constants.USER_DESCRIPTION));
				famous.put(Constants.USER_STATUSES_COUNT,
						user.get(Constants.USER_STATUSES_COUNT));
				famous.put(Constants.USER_FOLLOWERS_COUNT,
						user.get(Constants.USER_FOLLOWERS_COUNT));
				famous.put(Constants.USER_FRIENDS_COUNT,
						user.get(Constants.USER_FRIENDS_COUNT));
				famous.put(Constants.USER_FAVOURITES_COUNT,
						user.get(Constants.USER_FAVOURITES_COUNT));
				famous.put(Constants.USER_LOCATION,
						user.get(Constants.USER_LOCATION));
				// fetch star's tweets info
				sinceId = famousMgDao.querySinceId(uid);
			} else {
				famous = dbFamous.toMap();
				if (famous.containsKey(Constants.FAMOUS_NEWEST_TWEET)) {
					sinceId = (Long) famous.get(Constants.FAMOUS_NEWEST_TWEET);
				}
				famous.remove(Constants.FAMOUS_FRIENDS);
			}
			String fid = String.valueOf(famous.get(Constants.FAMOUS_ID));
			famous.put(Constants.FAMOUS_TWEETS,tweetFetcher.fetchTweetsByUserId(fid, sinceId));
			famous.put(Constants.FAMOUS_FRIENDS,userFetcher.fetchFriendsById(fid,200));
//					this.fetchTweetsByUserName(name));
		} catch (Exception e) {
			e.printStackTrace();
			log.warn(e);
		}
		famous.putAll(datum.getMetadata());
		datum.setMetadata(famous);
	}

	private JSONArray fetchTweetsByUserName(String name) {
		JSONArray result = new JSONArray();
		for (int page = 1; page <= maxPage; page++) {
			String url = String.format(starTweetsUrl, UrlUtils.encode(name),page);
			List<String> ids = new ArrayList<String>();
			boolean have = getStatusIds(ids, fetchPageStr(url));
			if (!have) {
				break;
			}
			int exists = 0;
			for (String id : ids) {
				if (tweetsMgDao.isTweetExists(id)) {
					exists++;
					break;
				}
				Map<String, Object> tweet = this.getTweet(id);
				result.add(tweet);
			}
			if (exists >= 5) {
				break;
			}
		}
		return result;
	}

	private String fetchPageStr(String url) {
		this.sleep(htmlWait);
		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(url);
		return page.getContentHtml();
	}

	private boolean getStatusIds(List<String> result, String html) {
		if (StringUtils.isEmpty(html)) {
			html = "";
		}
		return getAllMatchPattern(result, "&mid=(\\d*)", html);
	}

	private boolean getAllMatchPattern(List<String> result, String regex,
			String input) {
		boolean have = false;
		Pattern pat = PatternUtils.getPattern(regex);
		Matcher matcher = pat.matcher(input);
		while (matcher.find()) {
			for (int i = 0; i < matcher.groupCount(); i++) {
				String id = matcher.group(i + 1);
				if (!result.contains(id)) {
					result.add(id);
				}
			}
			have = true;
		}
		return have;
	}

	protected void sleep(int s) {
		try {
			Thread.sleep(s * 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * fetch next page
	 * 
	 * @param id
	 * @return
	 */
	private Document fetchPage(String url) {
		String content = this.fetchPageStr(url);
		Document doc = parseHtmlDocument(content);
		return doc;
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

	public Map<String, Object> getTweet(String id) {
		this.sleep(apiWait);
		return tweetFetcher.fetchOneTweet(id);
	}

}
