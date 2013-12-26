package com.sdata.live.fetcher.weibo;

import java.util.HashMap;
import java.util.Map;

import weibo4j.WeiboHelper;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.context.config.Configuration;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.live.fetcher.LiveBaseWithTime;
import com.sdata.proxy.item.SenseCrawlItem;
import com.sdata.proxy.resource.Resources;

/**
 * @author zhufb
 * 
 */
public abstract class LiveWeiboBase implements LiveBaseWithTime {

	protected String host = "http://www.weibo.com/";
	protected static Map<String, String> httpHeader = new HashMap<String, String>();
	protected static HttpPageLoader advancePageLoader = HttpPageLoader
			.getAdvancePageLoader();

	public static LiveWeiboBase getSenseFrom(SenseCrawlItem item,
			Configuration conf) {
		if (item.containParam(CrawlItemEnum.KEYWORD.getName())) {
			return new LiveWeiboFromWord(conf);
		} else if (item.containParam(CrawlItemEnum.ACCOUNT.getName())) {
			return new LiveWeiboFromUser();
		}
		return null;
	}

	public boolean isValid(String html) {
		if (StringUtils.isEmpty(html)) {
			return false;
		}
		if (html.contains("$CONFIG['islogin'] = '0'")) {
			return false;
		}
		if (html.contains("你的行为有些异常，请输入验证码")) {
			return false;
		}
		return true;
	}

	public void refreshResource() {
		httpHeader.put("Cookie", Resources.Weibo.get().getCookie());
	}

	protected String getTweetUrl(Map<String, Object> matadata) {
		StringBuffer sb = new StringBuffer(host);
		Map user = (Map) matadata.get("user");
		if (user == null || user.size() == 0) {
			return null;
		}
		Object id = matadata.get("id");
		if (id == null || !StringUtils.isNum(id.toString())) {
			return null;
		}
		sb.append(user.get("id")).append("/")
				.append(WeiboHelper.id2Mid((Long) id));
		return sb.toString();
	}
}
