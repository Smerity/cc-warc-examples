package org.commoncrawl.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveRecord;

public class WordCounter {
	private static final Logger LOG = Logger.getLogger(WordCounter.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EMPTY_PAGE_TEXT,
		EXCEPTIONS
	}

	protected static class WordCountMapper extends Mapper<Text, ArchiveRecord, Text, LongWritable> {
		private StringTokenizer tokenizer;
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);

		@Override
		public void map(Text key, ArchiveRecord value, Context context) throws IOException {
			context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);

			try {
				System.out.println(key + " -- " + value.available());
				byte[] rawData = new byte[value.available()];
				value.read(rawData);
				String content = new String(rawData);
				System.out.println(content.substring(0, Math.min(50, content.length())));
				System.out.println(content.length());
				System.out.println();
				
				tokenizer = new StringTokenizer(content);
				if (!tokenizer.hasMoreTokens()) {
					context.getCounter(MAPPERCOUNTER.EMPTY_PAGE_TEXT).increment(1);
				} else {
					while (tokenizer.hasMoreTokens()) {
						outKey.set(tokenizer.nextToken());
						context.write(outKey, outVal);
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
