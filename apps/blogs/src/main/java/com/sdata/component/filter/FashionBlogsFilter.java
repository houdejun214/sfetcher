package com.sdata.component.filter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lakeside.core.utils.StringUtils;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.core.Configuration;
import com.sdata.core.util.ApplicationResourceUtils;

public class FashionBlogsFilter {
	
	private final String filterPath = "filterPath";
	
	private List<String> rules;
	
	private Pattern blogUrlPattern;
	
	public FashionBlogsFilter(Configuration conf){
		rules = new ArrayList<String>();
		String filterRulesPath = conf.get(filterPath);
		String path = ApplicationResourceUtils.getResourceUrl(CrawlConfigManager.class,filterRulesPath);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "utf-8"));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (StringUtils.isNotEmpty(line)) {
					rules.add(line.trim());
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public boolean filter(String text){
		boolean filterResult = false;
		for(String rule:rules){
			blogUrlPattern = Pattern.compile(rule,Pattern.CASE_INSENSITIVE);
			Matcher matcher = blogUrlPattern.matcher(text);
			if(matcher.find()){
				filterResult = true;
				break;
			}
		}
		return filterResult;
	}
}
