package com.sdata.core.parser.select;

import org.junit.Test;
import sun.jvm.hotspot.debugger.Page;

import java.util.Arrays;

public class SelectorFormattorTest {

	@Test
	public void test() {
		
		String format = "test is aogoo {name}, title is {title}, array is {0}";
		SelectorFormattor formattor = new SelectorFormattor(format);
		PageContext context= new PageContext(null);
//		context.put("name", "qmm");
//		context.put("title", "professor");
//
//		String res = formattor.format(Arrays.asList("string1", "string2"), context);
//		System.out.println(format);
//		System.out.println(res);
	}

}
