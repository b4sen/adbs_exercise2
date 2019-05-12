package adbs.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyToMap = new Text();
	private Text valuesToMap = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO: new mapper: key: Title - value: author+checkouts
		//		 reducer: sum up checkouts, build author - title+checkouts
		// 		 2nd mapper: map author - title+checkouts
		//		 2nd reducer: max checkouts
		if (key.get() == 0) {
			return;
		} else {
			String[] row = CSVSplitter.split(value.toString());

			/*
			 * 
			 * Change the array indices to 5-6-7 when running the real job. The smaller
			 * dataset has an extra ID column, this caused the shift in the indices.
			 * 
			 */

//			int checkout = Integer.parseInt(row[6]);
			String checkout = row[5];
			String author = (row[7].isEmpty()) ? "AuthorMissing" : row[7];
			String title = row[6];

				keyToMap.set(title);
				valuesToMap.set(author + "#KOLOS#" + checkout);

				context.write(keyToMap, valuesToMap);
		}

	}
}
