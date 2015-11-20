package vghadoop.io;


import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RawFileRecordReader extends RecordReader<Text, BytesWritable>{

	FSDataInputStream fsdis = null;
	private Text key = new Text();
	private BytesWritable value = new BytesWritable();

	int pos = 0;
	int fsize = 0;

	@Override
	public void close() throws IOException {
		fsdis.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		FileSystem fs = FileSystem.get(context.getConfiguration());

		fsize = (int) split.getLength();
		fsdis = fs.open(split.getPath());
		
//		key = new Text( split.getPath().getName() );
		key = new Text( split.getPath().toString() );
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		boolean hasNext = false;

		if (pos < fsize) {
			hasNext = true;

			byte[] key_b = new byte[fsize];
			fsdis.readFully(key_b, pos, fsize);

			value = new BytesWritable(key_b);

			pos += fsize;
		}
		return hasNext;
	}

}
