package job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Main {
	
	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "Main");

		job.setJarByClass(Main.class);
		
		job.setMapperClass(FirstMapper.class);
				
		job.setReducerClass(FirstReducer.class);

		FileInputFormat.addInputPath(job, new Path("input/prices_50mila.csv"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapOutputKeyClass(TickerDate.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FirstReducerOutputValues.class);

		job.waitForCompletion(true);
		
	}

}
