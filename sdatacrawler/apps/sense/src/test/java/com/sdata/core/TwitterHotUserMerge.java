package com.sdata.core;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;

import com.sdata.core.TwitterHotUser.User;
import com.sdata.core.TwitterHotUser.UserComparator;

/**
 * @author zhufb
 *
 */
public class TwitterHotUserMerge {
	
	private static int MAX = 5000;
	private static String FILE1 = "output/twitterHotUserHbase.txt";
	private static String FILE2 = "output/twitterHotUser.txt";
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		List<String> flines1 = FileUtils.readLines(new File(FILE1), null);
		List<String> flines2 = FileUtils.readLines(new File(FILE2), null);
		flines1.addAll(flines2);
		List<User> list = new ArrayList<TwitterHotUser.User>();
		UserComparator uc = new UserComparator();
		for(String l:flines1){
			String replaceAll = l.replaceAll(" ", "").trim();
			replaceAll = replaceAll.substring(1,replaceAll.length() -1);
			String[] split = replaceAll.split(",",3);
			Object uid = split[0].split(":")[1];
			Object uname = split[1].split(":")[1];
			Object follows = split[2].split(":")[1];
			User u = new User(Long.valueOf(uid.toString()), uname.toString(), Long.valueOf(follows.toString()));
			list.add(u);
		}
		Collections.sort(list, uc);
		List<User> result = new ArrayList<User>();
		List<Object> ids = new ArrayList<Object>();
		for(User u:list){
			if(!ids.contains(u.getId())&&result.size()<MAX){
				ids.add(u.getId());
				result.add(u);
			}
			if(result.size()>=MAX){
				break;
			}
		}
		File file = getFile();
		for(User u:result){
			FileUtils.writeStringToFile(file, u.toString().concat("\r\n"), true);
		}
	}
	
	private static File getFile(){
		String f = "output/twitterHotUserAll.txt";
		com.lakeside.core.utils.FileUtils.delete(f);
		com.lakeside.core.utils.FileUtils.insureFileExist(f);
		return new File(f);
	}
}
