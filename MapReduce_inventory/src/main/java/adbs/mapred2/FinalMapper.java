package adbs.mapred2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text keyT = new Text();
	private Text valT = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] row = value.toString().split("#SOLOK#");
		String author = row[0];
		String title = row[1];
		
		keyT.set(title);
		valT.set(author+"#AUTHTOKEN#");
		
		context.write(keyT, valT);
		
	}

}
