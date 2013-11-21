package com.sdata.component.parser;


import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;

public class TweetsParser extends SdataParser {

	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		return result;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		return result;
	}

}
