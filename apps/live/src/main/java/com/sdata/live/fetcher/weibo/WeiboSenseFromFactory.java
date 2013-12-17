package com.sdata.live.fetcher.weibo;

import com.sdata.context.config.Configuration;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class WeiboSenseFromFactory {

	public static WeiboSenseFrom getSenseFrom(SenseCrawlItem item,
		Configuration conf) {
		if (item.containParam(CrawlItemEnum.KEYWORD.getName())) {
			return new WeiboSenseFromWord(conf);
		} else if (item.containParam(CrawlItemEnum.ACCOUNT.getName())) {
			return new WeiboSenseFromUser();
		}
		return null;
	}
}
