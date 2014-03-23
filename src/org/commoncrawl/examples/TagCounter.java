package org.commoncrawl.examples;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

public class TagCounter {
	private static final Logger LOG = Logger.getLogger(TagCounter.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}

	protected static class TagCounterMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);
		// The HTML regular expression is case insensitive (?i), avoids closing tags (?!/),
		// tries to find just the tag name before any spaces, and then consumes any other attributes.
		//private static final String HTML_TAG_PATTERN = "(?i)<(?!/)([^ >]+)([^>]*)>";
		private static final String HTML_TAG_PATTERN = "(?i)<(?!/)([^\\s>]+)([^>]*)>";
		private Pattern patternTag;
		private Matcher matcherTag;

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException {
			patternTag = Pattern.compile(HTML_TAG_PATTERN);
			
			for (ArchiveRecord r : value) {
				try {
					LOG.debug(r.getHeader().getUrl() + " -- " + r.available());
					if (r.getHeader().getMimetype().equals("application/http; msgtype=response")) {
						byte[] rawData = IOUtils.toByteArray(r, r.available());
						String content = new String(rawData);
						String headerText = content.substring(0, content.indexOf("\r\n\r\n"));
						System.out.println("=-=-=-=-=");
						System.out.println(headerText);
						System.out.println("---------");
						// TODO: Proper HTTP header parsing + don't trust headers
						if (headerText.contains("Content-Type: text/html")) {
							context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
							String body = content.substring(content.indexOf("\r\n\r\n") + 4);
							matcherTag = patternTag.matcher(body);
							while (matcherTag.find()) {
								String tagName = matcherTag.group(1);
								outKey.set(tagName.toLowerCase());
								context.write(outKey, outVal);
							}
						}
					}
				}
				catch (Exception ex) {
					LOG.error("Caught Exception", ex);
					context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
				}
			}
		}
	}
}
