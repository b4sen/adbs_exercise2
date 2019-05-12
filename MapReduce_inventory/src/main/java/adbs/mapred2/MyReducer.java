package adbs.mapred2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

	private Text maxT = new Text();
	private Text authText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int maxCheckout = 0;
		String author = "";

		for(Text v: values) {
			String[] token = v.toString().split("#KOLOS#");
			maxCheckout += Integer.parseInt(token[1]);

			if(author == "") {
				if(!token[0].isEmpty()) {
					if(!token[0].equalsIgnoreCase("AuthorMissing")) {
						author = token[0];
					}else {
						return;
					}
				}
			}
		}
		
		
		
		if (maxCheckout > 0) {
				maxT.set(key.toString() + "#KOLOS#" + maxCheckout);
				authText.set(author);
				context.write(authText, maxT);

			
		}

	}

}
