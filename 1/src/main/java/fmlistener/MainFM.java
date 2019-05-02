package fmlistener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainFM {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "FMlistener");

		job.setJarByClass(MainFM.class);
		
		job.setMapperClass(MapperFM.class);
		job.setReducerClass(ReducerFM.class);

		FileInputFormat.addInputPath(job, new Path("input/FMlistener"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}