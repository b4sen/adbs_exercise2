package adbs.mapred2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LibMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyT = new Text();
	private Text valT = new Text();

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		if (key.get() == 0) {
			return;
		} else {
			String[] row = CSVSplitter.split(values.toString());
			String title = row[1];
			String pubYear = row[4];
			String subject = row[6];
			String iLoc = row[10];

			keyT.set(title);
			valT.set(pubYear + "#KOLOS#" + subject + "#KOLOS#" + iLoc);
			context.write(keyT, valT);

		}
	}

}
