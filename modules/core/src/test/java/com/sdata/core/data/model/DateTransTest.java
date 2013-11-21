package com.sdata.core.data.model;

import org.junit.Test;

import com.lakeside.core.utils.time.DateFormat;

public class DateTransTest {

	@Test
	public void test() {
		System.out.println(DateFormat.changeStrToDate("Sat Apr 14 23:53:01 SGT 2012"));
	}

}
