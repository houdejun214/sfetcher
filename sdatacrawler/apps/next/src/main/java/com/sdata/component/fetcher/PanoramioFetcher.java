package com.sdata.component.fetcher;

import java.math.BigDecimal;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.Assert;
import com.lakeside.core.utils.StringUtils;
import com.sdata.component.parser.PanoramioParser;
import com.sdata.component.site.PanoramioApi;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.Location;
import com.sdata.core.PageRegionArea;
import com.sdata.core.RawContent;
import com.sdata.core.RegionArea;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.util.WebPageDownloader;

public class PanoramioFetcher extends SdataFetcher {
	
	private PanoramioRegionAreaIterator iterator;
	
	private PanoramioApi api;
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.PanoramioFetcher");
	
	public PanoramioFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		api = new PanoramioApi();
		parser = new PanoramioParser(conf,state);
		iterator = new PanoramioRegionAreaIterator(conf);
		String currentFetchState = state.getCurrentFetchState();
		PageRegionArea area = PageRegionArea.convert(currentFetchState);
		iterator.init(area);
	}
	
	@Override
	public List<FetchDatum> fetchDatumList() {
		if(iterator.haveNext()){
			PageRegionArea regionArea = iterator.getNextPageRegionArea();
		    String state = regionArea.toString();
			String queryUrl = api.getQueryUrl(regionArea);
			try {
				String content = WebPageDownloader.download(queryUrl);
				ParseResult parseList = parser.parseList(new RawContent(content));
				if(parseList.isListEmpty()){
					iterator.setIsLastPage(regionArea);
				}
				List<FetchDatum> fetchList = parseList.getFetchList();
				log.info("fetching page list ["+state+"],result:【"+fetchList.size()+"】");
				for(FetchDatum datum: fetchList){
					//datum.addMetadata("CurQuery", curQuery);
					datum.setCurrent(state);
				}
				return fetchList;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		if(datum!=null && !StringUtils.isEmpty( datum.getUrl())){
			String url = datum.getUrl();
			log.debug("fetching "+url);
			String content = WebPageDownloader.download(url);
			//datum.setContent(content);
			RawContent rawContent = new RawContent(url,content);
			ParseResult result = parser.parseSingle(rawContent);
			if(result != null){
				datum.setMetadata(result.getMetadata());
			}else{
				throw new RuntimeException("fetch content is empty");
			}
		}
		return datum;
	}

	@Override
	protected void moveNext() {
		
	}
	
	@Override
	public boolean isComplete() {
		boolean isComplete = !iterator.haveNext();
		if(isComplete){
			iterator.reset();
			state.setCurrentFetchState(null);
		}
		return isComplete;
	}
}

/*
 * the iterator of the region
 */
class PanoramioRegionAreaIterator {

	private static Location southwest;
	private static Location northeast;
	private static final BigDecimal regionLatitudeOffset = new BigDecimal(
			"0.000924");
	private static final BigDecimal regionLongitudeOffset = new BigDecimal(
			"0.000937");

	private static RegionArea currentArea = null;

	public PanoramioRegionAreaIterator(Configuration conf) {
		// ConfigSetting setting = ConfigSetting.instance(taskId);
		southwest = conf.getLocation("southwest");
		northeast = conf.getLocation("northeast");
		Assert.notNull(southwest,
				"the setting value of southwest location is empty!");
		Assert.notNull(northeast,
				"the setting value of northeast location is empty!");
	}

	public void init(String cityName) {
		if (currentArea == null) {
			currentArea = new RegionArea(southwest, regionLatitudeOffset,
					regionLongitudeOffset);
		}
	}

	public void init(PageRegionArea pageRegionArea) {
		if (currentArea == null) {
			if (pageRegionArea != null) {
				currentArea = pageRegionArea;
			} else {
				currentArea = new PageRegionArea(southwest,
						regionLatitudeOffset, regionLongitudeOffset);
			}
		}
	}

	/**
	 * reset to start from south west point
	 */
	public void reset() {
		currentArea = new PageRegionArea(southwest,
				regionLatitudeOffset, regionLongitudeOffset);
	}

	public PageRegionArea getNextPageRegionArea() {
		PageRegionArea currentPageArea = (PageRegionArea) currentArea;
		try {
			if (currentPageArea.isLastPage()) {
				if (currentPageArea.getMaxX().compareTo(
						northeast.getLongitude()) > 0) {
					Location newp = new Location(currentArea.getLb()
							.getLatitude(), southwest.getLongitude());
					currentArea = new PageRegionArea(newp,
							regionLatitudeOffset, regionLongitudeOffset);
					currentArea.move(regionLatitudeOffset, BigDecimal.ZERO);
				} else {
					currentArea.move(BigDecimal.ZERO, regionLongitudeOffset);
				}
			}
			PageRegionArea curRegion = currentPageArea.clone();
			currentPageArea.moveNextPage();
			return curRegion;

		} finally {
		}
	}

	public void setIsLastPage(PageRegionArea regionArea) {
		if (currentArea != null && regionArea != null) {
			PageRegionArea currentPageArea = (PageRegionArea) currentArea;
			if (currentPageArea.getLb().compareTo(regionArea.getLb()) == 0) {
				currentPageArea.setLastPage(true);
			}
		}
	}

	public boolean haveNext() {
		if (currentArea.getMaxX().compareTo(northeast.getLongitude()) > 0
				&& currentArea.getMaxY().compareTo(northeast.getLatitude()) > 0) {
			if (currentArea instanceof PageRegionArea) {
				PageRegionArea pageArea = (PageRegionArea) currentArea;
				if (pageArea.isLastPage()) {
					return false;
				} else {
					return true;
				}
			}
			return false;
		}
		return true;
	}
}
