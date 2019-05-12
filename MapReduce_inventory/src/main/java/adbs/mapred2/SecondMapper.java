package adbs.mapred2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyToMap = new Text();
	private Text valuesToMap = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] row = value.toString().split("#SOLOK#");

		String titleAndCheckout = row[1];
		String author = (row[0].isEmpty()) ? "AuthorMissing" : row[0];

		keyToMap.set(author);
		valuesToMap.set(titleAndCheckout);

		context.write(keyToMap, valuesToMap);
	}
}
