package com.sdata.crawl.cluster;

import java.util.List;

public interface Manager {

	public abstract void updateCrawlers(List<String> list);

	public abstract void updateCrawlerWithState(CrawlerState state);

}