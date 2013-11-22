package com.sdata.core;

/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.TException;
import org.junit.Test;

import com.framework.config.hbase.HCluster;
import com.framework.config.hbase.HClusterConfig;
import com.framework.db.hbase.HBaseConfig;
import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;

/*
 * Instructions:
 * 1. Run Thrift to generate the java module HBase
 *    thrift --gen java ../../../src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift
 * 2. Acquire a jar of compiled Thrift java classes.  As of this writing, HBase ships 
 *    with this jar (libthrift-[VERSION].jar).  If this jar is not present, or it is 
 *    out-of-date with your current version of thrift, you can compile the jar 
 *    yourself by executing {ant} in {$THRIFT_HOME}/lib/java.
 * 3. Compile and execute this file with both the libthrift jar and the gen-java/ 
 *    directory in the classpath.  This can be done on the command-line with the 
 *    following lines: (from the directory containing this file and gen-java/)
 *    
 *    javac -cp /path/to/libthrift/jar.jar:gen-java/ DemoClient.java
 *    mv DemoClient.class gen-java/org/apache/hadoop/hbase/thrift/
 *    java -cp /path/to/libthrift/jar.jar:gen-java/ org.apache.hadoop.hbase.thrift.DemoClient
 * 
 */
public class HBaseThirftDemo {

    static protected int port ;
    static protected String host;
    CharsetDecoder decoder = null;
	static String tableName = "thrift";
	static List<String> list = new ArrayList<String>();
		
	@Test
    public void test()
            throws IOError, TException, UnsupportedEncodingException, IllegalArgument, AlreadyExists {
        HBaseThirftDemo client = new HBaseThirftDemo();
        client.run();
    }

	public HBaseThirftDemo() {
        decoder = Charset.forName("UTF-8").newDecoder();
    }


    // Helper to translate byte[]'s to UTF8 strings
    private String utf8(byte[] buf) {
        try {
            return decoder.decode(ByteBuffer.wrap(buf)).toString();
        } catch (CharacterCodingException e) {
            return "[INVALID UTF-8]";
        }
    }

    // Helper to translate strings to UTF8 bytes
    private byte[] bytes(String s) {
        try {
            return s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void run() throws IOError, TException, IllegalArgument,
            AlreadyExists {
    	
		String namespace = "Test";
		String cf1 = "dcf";
		String cf2 = "others";
		int RECORDS = 100;
		// config
    	HCluster cluster = HClusterConfig.getInstance().getCluster();
    	HBaseConfig config = new HBaseConfig(cluster.getConfiguration());
    	
    	// get hbase thrift client
		HBaseClient client = HBaseClientFactory.getClientWithNormalSeri(namespace); 
 		if(!client.exists(tableName)){
			//create table
			client.createTable(tableName, cf1,cf2);
		}
		
 		for(int i =0;i<RECORDS;i++){
 			Thread t = new Thread(new ThriftClient(client));
 			t.start();
		}
//		
//		for(String pk:list){
//			Map<String, Object> query = client.query(tableName, pk);
//			printMap(query);
//		}
//
//		Map<String, Map<String, Object>> query = client.query(tableName, list);
//		printMap(query);
//		
//		List<TRowResult> queryTRows = client.queryTRows(tableName, list);
//		printRow(queryTRows);
    }

    private final void printVersions(ByteBuffer row, List<TCell> versions) {
        StringBuilder rowStr = new StringBuilder();
        for (TCell cell : versions) {
            rowStr.append(utf8(cell.value.array()));
            rowStr.append("; ");
        }
        System.out.println("row: " + utf8(row.array()) + ", values: " + rowStr);
    }

    private final void printRow(TRowResult rowResult) {
        // copy values into a TreeMap to get them in sorted order
        TreeMap<String, TCell> sorted = new TreeMap<String, TCell>();
        for (Map.Entry<ByteBuffer, TCell> column : rowResult.columns.entrySet()) {
            sorted.put(utf8(column.getKey().array()), column.getValue());
        }

        StringBuilder rowStr = new StringBuilder();
        for (SortedMap.Entry<String, TCell> entry : sorted.entrySet()) {
            rowStr.append(entry.getKey());
            rowStr.append(" => ");
            rowStr.append(utf8(entry.getValue().value.array()));
            rowStr.append("; ");
        }
        System.out.println("row: " + utf8(rowResult.row.array()) + ", cols: " + rowStr);
    }

    private void printRow(List<TRowResult> rows) {
        for (TRowResult rowResult : rows) {
            printRow(rowResult);
        }
    }

    private void printMap(Map<?,?> rows) {
        for (Map.Entry<?,?> e : rows.entrySet()) {
        	if(e instanceof Map){
        		System.out.println(" row:" + e.getKey() +"..........");
        		printMap((Map)e);
        	}else{
            	System.out.println("col:"+e.getKey() +", val:" + e.getValue());
        	}
        }
    }
    
   static class ThriftClient implements Runnable{
	private HBaseClient client;
	public ThriftClient(HBaseClient client){
		this.client = client;
	}
	
	public void run() {
		HashMap<String, Object> map = new HashMap<String,Object>();
		String pk = String.valueOf(Math.random()*1000);
		map.put("id", pk);
		map.put("name","test");
		map.put("date", new Date());
		map.put("short", (short)1);
		map.put("int", 2);
		map.put("long", 3l);
		map.put("float", 4.2f);
		map.put("double", 5.32456);
		map.put("bigdecimal", new BigDecimal(2.10112));
		map.put("boolean", true);
		client.save(tableName,pk,map);
		list.add(pk);
	}
	   
   }
    
}
