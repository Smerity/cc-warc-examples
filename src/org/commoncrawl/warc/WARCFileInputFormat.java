package org.commoncrawl.warc;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.archive.io.ArchiveRecord;


public class WARCFileInputFormat extends FileInputFormat<Text, ArchiveRecord> {

	@Override
	public RecordReader<Text, ArchiveRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new WARCFileRecordReader();
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
}
