package org.commoncrawl.examples.mapreduce;

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

public class TagCounterMap {
	private static final Logger LOG = Logger.getLogger(TagCounterMap.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}

	protected static class TagCounterMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);
		// The HTML regular expression is case insensitive (?i), avoids closing tags (?!/),
		// tries to find just the tag name before any spaces, and then consumes any other attributes.
		private static final String HTML_TAG_PATTERN = "(?i)<(?!/)([^\\s>]+)([^>]*)>";
		private Pattern patternTag;
		private Matcher matcherTag;

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException {
			// Compile the regular expression once as it will be used continuously
			patternTag = Pattern.compile(HTML_TAG_PATTERN);
			
			for (ArchiveRecord r : value) {
				try {
					LOG.debug(r.getHeader().getUrl() + " -- " + r.available());
					// We're only interested in processing the responses, not requests or metadata
					if (r.getHeader().getMimetype().equals("application/http; msgtype=response")) {
						// Convenience function that reads the full message into a raw byte array
						byte[] rawData = IOUtils.toByteArray(r, r.available());
						String content = new String(rawData);
						// The HTTP header gives us valuable information about what was received during the request
						String headerText = content.substring(0, content.indexOf("\r\n\r\n"));
						
						// In our task, we're only interested in text/html, so we can be a little lax
						// TODO: Proper HTTP header parsing + don't trust headers
						if (headerText.contains("Content-Type: text/html")) {
							context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
							// Only extract the body of the HTTP response when necessary
							// Due to the way strings work in Java, we don't use any more memory than before
							String body = content.substring(content.indexOf("\r\n\r\n") + 4);
							// Process all the matched HTML tags found in the body of the document
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
