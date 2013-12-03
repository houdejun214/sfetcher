package test;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.future.FutureIDBuilder;

/**
 * @author zhufb
 * 
 */
public class WeiboAreaHotUserLoader {

	private static String MAINPAGE = "http://verified.weibo.com/fame/jiangsu/?rt=3&srt=4&letter={0}&province=32&city=1&page={1}";
	private static String table = "nanjing_weibo_users";

	private static List<String> Letters = Arrays.asList("a", "b", "c", "d",
			"e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
			"r", "s", "t", "u", "v", "w", "x", "y", "z");

//	private static HBaseClient client = HBaseClientFactory
//			.getClientWithCustomSeri("next-2", "future");

	public static void main(String[] args) throws IOException {
		
		// getCookie();
//		if (!client.exists(table)) {
//			client.createTable(table, "dcf");
//		}

		List<String> result = new ArrayList<String>();
		for (String letter : Letters) {
			int p = 1;
			while (true) {
				String url = MessageFormat.format(MAINPAGE, letter, p);
				String html = fetchPage(url);
				getUserIds(result, html);
				if (!haveNext(html)) {
					break;
				}
				p++;
			}
		}

		saveUsers(result);
	}

	private static boolean getUserIds(List<String> result, String html) {
		if (StringUtils.isEmpty(html)) {
			html = "";
		}
		return getAllMatchPattern(result, "action-data=\\\\\"uid=(\\d*)\\\\\"", html);
	}

	private static boolean haveNext(String html) {
		if (StringUtils.isEmpty(html)) {
			html = "";
		}
		return html.contains("\u4e0b\u4e00\u9875");
	}

	private static boolean getAllMatchPattern(List<String> result,
			String regex, String input) {
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

	private static void saveUsers(List<String> users) throws IOException {
		StringBuffer sb = new StringBuffer();
		for (String uid : users) {
			sb.append(uid).append("\r\n");
			File file = getFile();
			FileUtils.writeStringToFile(file, sb.toString());
		}
		System.out.println("save user size:" + users.size());
	}


	private static File getFile() {
		String f = "output/".concat(table).concat(".txt");
		com.lakeside.core.utils.FileUtils.delete(f);
		com.lakeside.core.utils.FileUtils.insureFileExist(f);
		return new File(f);
	}

	private static String fetchPage(String url) {
		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(url);
		return page.getContentHtml();
	}
}
