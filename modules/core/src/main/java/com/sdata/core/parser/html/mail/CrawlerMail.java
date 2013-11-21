package com.sdata.core.parser.html.mail;

import com.sdata.core.CrawlAppContext;
import com.sdata.core.util.EmailUtil;

/**
 * @author zhufb
 *
 */
public class CrawlerMail {
	final static Integer TIMES = 3;

	public static void send(String subject,String content){
		int time = 1;
		synchronized (TIMES) {
			while(true){
				try {
					EmailUtil.send(getEmails(),subject ,content);
					return;
				} catch (Exception e) {
					e.printStackTrace();
					time++;
					if(time>TIMES) return;
				}
			}
		}
    } 
	
	public static void send(){
		send(getCrwalerStopContent());
    } 
	
	public static void send(String content){
		send("Crawler waring",content);
    } 
	
	private static String getCrwalerStopContent(){
		StringBuffer subject = new StringBuffer();
		subject.append("Warning:").append("Crawler 【");
		subject.append(CrawlAppContext.state.getCrawlName()).append("】");
		subject.append(" at port 【").append(CrawlAppContext.startServerPort);
		subject.append("】  has stoped  just now !!!");
		return subject.toString();
	}
	
	private static String[] getEmails(){
		String string = CrawlAppContext.conf.get("emails");
		String[] split = string.split(",");
		return split;
	}

 }