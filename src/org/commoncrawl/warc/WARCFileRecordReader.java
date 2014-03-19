package org.commoncrawl.warc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;


public class WARCFileRecordReader extends RecordReader<Text, ArchiveRecord> {
	private ArchiveReader ar;
	private FSDataInputStream fsin;
	private Iterator<ArchiveRecord> iter;
	private ArchiveRecord currentRecord;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		fsin = fs.open(path);
		ar = WARCReaderFactory.get(path.getName(), fsin, true);
		iter = ar.iterator();
		nextKeyValue();
	}
	
	@Override
	public void close() throws IOException {
		currentRecord.close();
		fsin.close();
		ar.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		if (currentRecord != null) {
			return new Text(currentRecord.getHeader().getUrl());
		}
		return null;
	}

	@Override
	public ArchiveRecord getCurrentValue() throws IOException, InterruptedException {
		return currentRecord;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return iter.hasNext() ? 0 : 1;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!iter.hasNext()) {
			return false;
		}
		return true;
	}
}