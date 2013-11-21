package com.sdata.core.parser.html.field;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.sdata.core.CrawlAppContext;


/**
 * @author zhufb
 *
 */
public class StrategyInit extends StrategyField {
	protected String link;
	protected String file;
	protected String initFrom;
	private List<String> list = new ArrayList<String>();
	
	public String getNextInit() {
		String result = null;
		int currentEntry = CrawlAppContext.state.getCurrentEntry();
		if(list.size() > currentEntry){
			result =  list.get(currentEntry++);
			CrawlAppContext.state.setCurrentEntry(currentEntry);
		}
		return result;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
		list.add(link);
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
		if (StringUtils.isEmpty(file)) {
			return;
		}
		File initFile = new File(file);
		if (initFile.exists()) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(initFile));
				String line = br.readLine();
				while (line != null ) {
					list.add(line);
					line = br.readLine();
				}
				br.close();
			} catch (IOException e) {
				throw new RuntimeException("init file IOException",e);
			}
		}
	}

	public String getInitFrom() {
		return initFrom;
	}

	public void setInitFrom(String initFrom) {
		this.initFrom = initFrom;
		if(StringUtils.isEmpty(initFrom)){
			return;
		}
		try {
			Class<?> classType = Class.forName(initFrom);
			Object classInstance = classType.newInstance();
			Method method = classType.getMethod("getEntryList", new Class[]{});
		} catch (Exception e) {
			throw new RuntimeException("init from class exception",e);
		}
	}

	public String getName() {
		return Tags.INIT.getName();
	}

}
