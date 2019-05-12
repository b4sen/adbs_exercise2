package adbs.mapred2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import adbs.mapred2.Main;
import adbs.mapred2.MyMapper;
import adbs.mapred2.MyReducer;
import adbs.mapred2.SecondMapper;
import adbs.mapred2.SecondReducer;

public class Main {

	public static void main(String[] args) throws Exception {

		Path p1 = new Path(args[0]);
		Path p2 = new Path(args[1]);
		Path out = new Path(args[2]);
		Path libraryIn = new Path(args[3]);
		Path finalOut = new Path(args[4]);
		
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", "#SOLOK#");
		Job job = Job.getInstance(conf, "ADBS_Mapred2");

		job.setJarByClass(Main.class);
		
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, p1);
		FileOutputFormat.setOutputPath(job, p2);
		
		p2.getFileSystem(conf).delete(p2, true);
		
		if(!job.waitForCompletion(true)) {
			System.exit(1);
		}
		
		/*
		 * System.exit(job.waitForCompletion(true) ? 0 : 1);
		 * 
		 */
		
		Configuration conf2 = new Configuration();
		conf2.set("mapred.textoutputformat.separator", "#SOLOK#");
		Job job2 = Job.getInstance(conf2, "Secondary Job");

		job2.setJarByClass(Main.class);
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		
		FileInputFormat.addInputPath(job2, p2);
		FileOutputFormat.setOutputPath(job2, out);
		
		out.getFileSystem(conf2).delete(out, true);
		
		
		if(!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		

		
		
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "FINAL JOB");
		
	
		job3.setJarByClass(Main.class);
		
		MultipleInputs.addInputPath(job3, out, TextInputFormat.class, FinalMapper.class);
		MultipleInputs.addInputPath(job3, libraryIn, TextInputFormat.class, LibMapper.class);
		job3.setReducerClass(FinalReducer.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setOutputFormatClass(TextOutputFormat.class);

		
		FileOutputFormat.setOutputPath(job3, finalOut);		
		finalOut.getFileSystem(conf3).delete(finalOut, true);
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);

	}

}
