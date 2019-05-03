package job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
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
		FileOutputFormat.setOutputPath(job, new Path("output/job1"));
		
		job.setMapOutputKeyClass(TickerDate.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FirstReducerOutputValues.class);

		job.waitForCompletion(true);
		
		//-------------------- job2---------
		
		Job job2 = new Job(new Configuration(), "Main2");

		job2.setJarByClass(Main.class);
		
		job2.setMapperClass(SecondMapper.class);
				
		job2.setReducerClass(SecondReducer.class);

		FileInputFormat.addInputPath(job2, new Path("output/job1"));
		FileOutputFormat.setOutputPath(job2, new Path("output/job2"));
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(FirstReducerOutputValues.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(FirstReducerOutputValues.class);

		job2.waitForCompletion(true);
		
		
		
		
	}

}
