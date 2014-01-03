package test;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;
import com.lakeside.core.utils.time.DateFormat;
import com.sdata.db.hbase.HBaseRawIndex;

/**
 * @author zhufb
 * 
 */
public class DataImport {

	private static String filePath = "/home/zhufb/Documents/facebook.xls";
	private static String source = "facebook";
	private static String tableName = "test_facebook";
	private static Long objectId = 103l;
	private static HBaseClient client = HBaseClientFactory
			.getClientWithCustomSeri("next-2", "di");

	public static void main(String[] args) {
		try {
			HBaseRawIndex hbaseGenID = new HBaseRawIndex("raw_index", client);
			client.createTable(tableName, "dcf");
			// t.xls为要读取的excel文件名
			Workbook book = Workbook.getWorkbook(new File(filePath));
			// 获得第一个工作表对象(ecxel中sheet的编号从0开始,0,1,2,3,....)
			Sheet sheet = book.getSheet(0);
			int rows = sheet.getRows();
			Cell[] header = sheet.getRow(0);
			for (int r = 1; r < rows; r++) {
				Cell[] data = sheet.getRow(r);
				if (data.length != header.length) {
					break;
				}
				Map<String, Object> raw = new HashMap<String, Object>();
				StringBuffer sb = new StringBuffer();
				for (int c = 0; c < header.length; c++) {
					String key = header[c].getContents();
					Object val = data[c].getContents();
					sb.append(val);
					if (key.contains("time")) {
						val = DateFormat.strToDate(val);
					}
					raw.put(key, val);
				}
				int id = sb.toString().hashCode();
				raw.put("fet_time", new Date());
				raw.put("dtf_oid", objectId);
				raw.put("dtf_s", source);
				raw.put("id", id);
				Long rk = hbaseGenID.getOrCreateRowKey(id);
				client.save(tableName, rk, raw);
			}
			book.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
