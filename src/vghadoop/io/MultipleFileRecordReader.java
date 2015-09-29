package vghadoop.io;


import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MultipleFileRecordReader extends EntireFileRecordReader {

	private int index = 0;

	public MultipleFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index)
			throws IOException, InterruptedException {
		this.index = index;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		CombineFileSplit csplit = (CombineFileSplit) split;

		FileSplit fsplit = new FileSplit(
				csplit.getPath(index),
				csplit.getOffset(index),
				csplit.getLength(index),
				csplit.getLocations());
		super.initialize(fsplit, context);
	}

}
