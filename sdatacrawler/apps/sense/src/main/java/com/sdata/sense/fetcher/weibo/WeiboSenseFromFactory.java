package com.sdata.sense.fetcher.weibo;

import java.util.Map;

import com.sdata.core.Configuration;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class WeiboSenseFromFactory {

	public static WeiboSenseFrom getSenseFrom(SenseCrawlItem item,
		Configuration conf, Map<String, String> httpHeader) {
		if (item.containParam(CrawlItemEnum.KEYWORD.getName())) {
			return new WeiboSenseFromWord(conf, httpHeader);
		} else if (item.containParam(CrawlItemEnum.ACCOUNT.getName())) {
			return new WeiboSenseFromUser();
		}
		return null;
	}
}
