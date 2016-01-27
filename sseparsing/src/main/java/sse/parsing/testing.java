package sse.parsing;

public class testing {

	public static void main( String[] args ) {
		String testing = "12345678eStRiN";
		System.out.println("Odd characters: " + displayOdd(testing));
		System.out.println("Even characters: " + displayEven(testing));
	}
	
	private static String displayOdd(String string) {
		String value = ""; //= string;
		char[] chrArray = string.toCharArray();
		for(int i=0;i<chrArray.length;i++) {
			if(i%2==0) {
				value = value + chrArray[i];
			}
		}
		
		return value;
	}
	
	private static String displayEven(String string) {
		String value = ""; //= string;
		char[] chrArray = string.toCharArray();
		for(int i=0;i<chrArray.length;i++) {
			if(i%2==1) {
				value = value + chrArray[i];
			}
		}
		
		return value;
	}
}
