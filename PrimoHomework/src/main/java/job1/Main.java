package job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
	
	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "Main");

		job.setJarByClass(Main.class);
		
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);

		FileInputFormat.addInputPath(job, new Path("input/prices_50mila.csv"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.waitForCompletion(true);
		
	}

}
