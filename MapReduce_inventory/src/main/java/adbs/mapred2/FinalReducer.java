package adbs.mapred2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalReducer extends Reducer<Text, Text, Text, Text> {

	private Text keyT = new Text();
	private Text valT = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String title = key.toString();
		String author = "";
		String pubYear = "";
		String subject = "";
		String iLoc = "";

		for (Text t : values) {
			if (t.toString().contains("#AUTHTOKEN#")) {
				author = t.toString().split("#AUTHTOKEN")[0];
			}

			if(!t.toString().contains("#AUTHTOKEN#")) {
				if (!author.equalsIgnoreCase("")) {
					String[] rows = t.toString().split("#KOLOS#");
					if (rows.length == 3) {
						pubYear = (rows[0].isEmpty()) ? "MISSING YEAR" : rows[0];
						subject = (rows[1].isEmpty()) ? "MISSING SUBJECT" : rows[1];
						iLoc = (rows[2].isEmpty()) ? "MISSING LOCATION" : rows[2];
					} else if (rows.length == 2) {
						pubYear = (rows[0].isEmpty()) ? "MISSING YEAR" : rows[0];
						subject = (rows[1].isEmpty()) ? "MISSING SUBJECT" : rows[1];
						iLoc = "MISSING LOCATION";
					} else if (rows.length == 1) {
						pubYear = (rows[0].isEmpty()) ? "MISSING YEAR" : rows[0];
						subject = "MISSING SUBJECT";
						iLoc = "MISSING LOCATION";
					} else {
						pubYear = "MISSING YEAR";
						subject = "MISSING SUBJECT";
						iLoc = "MISSING LOCATION";
					}
				} else {
					return;
				}
			}
			

		}

		if (!author.equalsIgnoreCase("")) {
			if(!pubYear.equalsIgnoreCase("") || !subject.equalsIgnoreCase("") || !iLoc.equalsIgnoreCase("")) {
				keyT.set(author);
				valT.set(title + " " + pubYear + " " + subject + " " + iLoc);
				context.write(keyT, valT);
			}else {
				return;
			}
			
		} else {
			return;
		}

	}
}
