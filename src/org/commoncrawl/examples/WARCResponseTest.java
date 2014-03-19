package org.commoncrawl.examples;

public class WARCResponseTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String txt = new String(rawData);
		// This gives us the HTTP headers and body
		// If you want to parse the headers, look towards Apache HttpComponents
		// http://hc.apache.org/index.html
		// We're just interested in the body, so skip to the end of the headers (<CR><LF>)
		System.out.println(rawData.length);
		System.out.println(txt.indexOf("\r\n\r\n"));
		// TODO: Ensure we can't walk off the end
		String body = txt.substring(txt.indexOf("\r\n\r\n") + 4);
		System.out.println("________________");
		System.out.println(body.substring(0, Math.min(body.length(), 500)));
	}

}
