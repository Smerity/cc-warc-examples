package org.commoncrawl.warc;

import java.io.IOException;

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
import org.archive.io.warc.WARCReaderFactory;


public class WARCFileRecordReader extends RecordReader<Text, ArchiveReader> {
	private String arPath;
	private ArchiveReader ar;
	private FSDataInputStream fsin;
	private boolean hasBeenRead = false;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		fsin = fs.open(path);
		arPath = path.getName();
		ar = WARCReaderFactory.get(path.getName(), fsin, true);
	}
	
	@Override
	public void close() throws IOException {
		fsin.close();
		ar.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text(arPath);
	}

	@Override
	public ArchiveReader getCurrentValue() throws IOException, InterruptedException {
		return ar;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// Progress of reader through the data as a float
		// As each file only produces one ArchiveReader, this will be one immediately
		return hasBeenRead ? 1 : 0;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (hasBeenRead) {
			return false;
		}
		hasBeenRead = true;
		return true;
	}
}