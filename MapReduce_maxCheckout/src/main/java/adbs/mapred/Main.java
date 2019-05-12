package adbs.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", "#SOLOK#");
		Job job = Job.getInstance(conf, "TITLE - AUTH + CHECKOUT");
		
		
		job.setJarByClass(Main.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		if(!job.waitForCompletion(true)) {
			System.exit(1);
		}
		
		/*
		 * System.exit(job.waitForCompletion(true) ? 0 : 1);
		 * 
		 */
		
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "FINAL JOB");

		job2.setJarByClass(Main.class);
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		Path secondOutputPath = new Path(args[2]);
		
		FileInputFormat.addInputPath(job2, outputPath);
		FileOutputFormat.setOutputPath(job2, secondOutputPath);
		
		secondOutputPath.getFileSystem(conf2).delete(secondOutputPath, true);
		
		
		if(job2.waitForCompletion(true)) {
			System.exit(1);
		}
		
		
	}

}
