package org.commoncrawl.examples.mapreduce;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.json.JSONException;
import org.json.JSONObject;

public class ServerTypeMap {
	private static final Logger LOG = Logger.getLogger(ServerTypeMap.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		NO_SERVER,
		EXCEPTIONS
	}

	protected static class ServerMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException {
			for (ArchiveRecord r : value) {
				// Skip any records that are not JSON
				if (!r.getHeader().getMimetype().equals("application/json")) {
					continue;
				}
				try {
					context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
					// Convenience function that reads the full message into a raw byte array
					byte[] rawData = IOUtils.toByteArray(r, r.available());
					String content = new String(rawData);
					JSONObject json = new JSONObject(content);
					try {
						String server = json.getJSONObject("Envelope").getJSONObject("Payload-Metadata").getJSONObject("HTTP-Response-Metadata").getJSONObject("Headers").getString("Server");
						outKey.set(server);
						context.write(outKey, outVal);
					} catch (JSONException ex) {
						// If we reach here, the JSON object didn't have the header we were looking for
						// There are likely better ways to check for json["Envelope"]["Payload-Metadata"][...] but this is concise
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
