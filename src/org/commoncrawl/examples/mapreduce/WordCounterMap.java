package org.commoncrawl.examples.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

public class WordCounterMap {
	private static final Logger LOG = Logger.getLogger(WordCounterMap.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EMPTY_PAGE_TEXT,
		EXCEPTIONS,
		NON_PLAIN_TEXT
	}

	protected static class WordCountMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {
		private StringTokenizer tokenizer;
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException {
			for (ArchiveRecord r : value) {
				try {
					if (r.getHeader().getMimetype().equals("text/plain")) {
						context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
						LOG.debug(r.getHeader().getUrl() + " -- " + r.available());
						// Convenience function that reads the full message into a raw byte array
						byte[] rawData = IOUtils.toByteArray(r, r.available());
						String content = new String(rawData);
						// Grab each word from the document
						tokenizer = new StringTokenizer(content);
						if (!tokenizer.hasMoreTokens()) {
							context.getCounter(MAPPERCOUNTER.EMPTY_PAGE_TEXT).increment(1);
						} else {
							while (tokenizer.hasMoreTokens()) {
								outKey.set(tokenizer.nextToken());
								context.write(outKey, outVal);
							}
						}
					} else {
						context.getCounter(MAPPERCOUNTER.NON_PLAIN_TEXT).increment(1);
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
