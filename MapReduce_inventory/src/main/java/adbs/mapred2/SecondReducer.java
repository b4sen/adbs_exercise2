package adbs.mapred2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<Text, Text, Text, Text>{
	
	private Text maxT = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int maxCheckout = 0;
		String maxTitle = null;
		
		while(values.iterator().hasNext()) {
			String[] tokens = values.iterator().next().toString().split("#KOLOS#");
			
			String title = tokens[0];
			int checkout = Integer.parseInt(tokens[1]);
			
			if(checkout > maxCheckout) {
				maxCheckout = checkout;
				maxTitle = title;
			}
			
		}
		
		if(!key.toString().equalsIgnoreCase("AuthorMissing")) {
			maxT.set(maxTitle);
			context.write(key, maxT);
		}else {
			return;
		}
		
		
	}

}
