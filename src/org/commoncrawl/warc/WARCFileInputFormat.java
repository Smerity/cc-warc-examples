package org.commoncrawl.warc;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.archive.io.ArchiveReader;

/**
 * Minimal implementation of FileInputFormat for WARC files.
 * Hadoop is told that splitting these compressed files is not possible.
 *
 * @author Stephen Merity (Smerity)
 */
public class WARCFileInputFormat extends FileInputFormat<Text, ArchiveReader> {

	@Override
	public RecordReader<Text, ArchiveReader> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new WARCFileRecordReader();
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// As these are compressed files, they cannot be (sanely) split
		return false;
	}
}
