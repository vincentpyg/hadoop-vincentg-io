package vghadoop.io;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class MultipleTextFileInputFormat extends CombineFileInputFormat<Text,Text> {

    public MultipleTextFileInputFormat(){
        super();
        this.setMaxSplitSize(5000000);
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<Text,Text>((CombineFileSplit)split, context, MultipleTextFileRecordReader.class);
    }
}
