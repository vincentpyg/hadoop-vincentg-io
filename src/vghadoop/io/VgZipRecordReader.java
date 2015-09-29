package vghadoop.io;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import com.google.common.base.Charsets;

/***
 * Read all text entries from a zip file;
 * @author vincentg
 *
 */
public class VgZipRecordReader extends RecordReader<Text, Text> {

	private static final Log _logger = LogFactory.getLog(VgZipRecordReader.class.getName());

	private ZipInputStream zis;
	private LineReader lineReader;

	private String curFile;
	private byte[] delimiterBytes;

	long start = 0;
	long pos = 0;
	long end = 0;

	private Text key = new Text();
	private Text value = new Text();


	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		FileSystem fs = FileSystem.get(context.getConfiguration());
		zis = new ZipInputStream( fs.open(split.getPath()) );


		String delimiter = context.getConfiguration().get("textinputformat.record.delimiter", "\n");
		delimiterBytes = delimiter.getBytes(Charsets.UTF_8);

		try {
			//ADVANCE TO FIRST ENTRY;
			ZipEntry entry = zis.getNextEntry();
			curFile = entry.getName();
			end = entry.getSize();
			lineReader = new LineReader(zis, delimiterBytes);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean hasNext = false;

		if (pos < end) {
			hasNext = true;
			key.set(curFile+"@"+pos);
			pos+=lineReader.readLine(value);
			_logger.debug("key="+key+", "+"value="+value);
		} else {
			ZipEntry entry;
			if ((entry = zis.getNextEntry()) != null) {
				hasNext = true;
				pos = 0;
				end = entry.getSize();
				curFile = entry.getName();
				key.set(curFile+"@"+pos);
				lineReader = new LineReader(zis, delimiterBytes);
				pos+=lineReader.readLine(value);
			}
		}
		return hasNext;
	}


	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}


	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}


	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start ==  end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos-start)/(float)(end-start));
		}
	}

	@Override
	public void close() throws IOException {
		zis.close();
		lineReader.close();
	}

}
