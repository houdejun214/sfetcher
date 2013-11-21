package com.sdata.core.util;

import java.util.Stack;

public class BooleanEvaluator {

 	public static boolean eval(String expression) {
 		Stack<Boolean> stack = new Stack<Boolean>();
 		Stack<Character> opers = new Stack<Character>();
 		char[] arr = expression.toCharArray();
 		for (int i = 0;i<arr.length;i++ ) {
 			char c = arr[i];
			switch(c){
	 			case '&':
	 				opers.push('&');
	 				continue;
	 			case '|':
	 				opers.push('|');
	 				continue;
	 			case '(':
	 				opers.push('(');
	 				continue;
	 			case ')':
	 				if(opers.pop() != '('){
	 					throw new RuntimeException("error");
	 				}
	 				if(!opers.empty() && opers.peek() == '&'){
	 					stack.push(stack.pop() && booleanValue(c));
	 					opers.pop();
	 				}else if(!opers.empty() && opers.peek() == '|'){
	 					stack.push(stack.pop() || booleanValue(c));
	 					opers.pop();
	 				}
	 				continue;
	 			default:
	 				if(!opers.empty() && opers.peek() == '&'){
	 					stack.push(stack.pop() && booleanValue(c));
	 					opers.pop();
	 				}else if(!opers.empty() && opers.peek() == '|'){
	 					stack.push(stack.pop() || booleanValue(c));
	 					opers.pop();
	 				}else{
	 					stack.push(booleanValue(c));
	 				}
 			}
 		}
 		return stack.pop();
 	}

 	private static boolean booleanValue(char b) {
 		if(b == '0'){
 			return false;
 		}else{
 			return true;
 		}
 	}

 	public static void main(String [] a){
// 		String exp = "1&0|1";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "1&1";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "0|1";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "0|0";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "1|1";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "0&0";
// 		System.out.println(exp+" = "+eval(exp));
// 		
// 		exp = "(1|1)&0";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "(1|0)&0|1";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "(1|0)&1";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "(1&0)&0";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "(1&1)&0";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "(0|0)&0";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "(1|1)&0";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "(1|1)&1";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "1&(1|0)";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "1&(0|0)";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "1&(1&0)";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "1&(1&(1&0))";
// 		System.out.println(exp+" = "+eval(exp));
// 		exp = "1&(1|(1&0))";
// 		System.out.println(exp+" = "+eval(exp));
 	}
 }