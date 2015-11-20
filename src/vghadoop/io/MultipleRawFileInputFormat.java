package vghadoop.io;


import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class MultipleRawFileInputFormat extends CombineFileInputFormat<Text,BytesWritable> {

    public MultipleRawFileInputFormat(){
        super();
//        this.setMaxSplitSize(5000000);
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<Text,BytesWritable>((CombineFileSplit)split, context, MultipleRawFileRecordReader.class);
    }
}
