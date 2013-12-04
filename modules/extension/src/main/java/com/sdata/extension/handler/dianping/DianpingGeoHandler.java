package com.sdata.extension.handler.dianping;

import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;

public class DianpingGeoHandler implements IParserHandler {

	public Object handle(IParserContext context, Object data) {

		if(data == null){
			return data;
		}
		String ciphertext = data.toString();
		int digi=16;
		int add= 10;
		int plus=7;
		int cha=36;
		int I = -1;
		int H = 0;
        String B = "";
        int J = ciphertext.length();
        char G = ciphertext.charAt(J-1);
        String C = ciphertext.substring(0, J-1);

        for (int E = 0; E < J-1; E++) {
        	int D = Integer.parseInt(String.valueOf(C.charAt(E)), cha) - add;
            if (D >= add) {
                D = D - plus;
            }
            B += Integer.toString(D, cha);
            if (D > H) {
                I = E;
                H = D;
            }
        }
        int A = Integer.parseInt(B.substring(0, I), digi);
        int F = Integer.parseInt(B.substring(I + 1), digi);
        
        double L = (A + F - G) / 2;
        double K = (F - L) / 100000;
        L /= 100000;
        return String.valueOf(K) + "," + String.valueOf(L);
	}

}
