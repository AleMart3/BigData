package bigram;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Main {
	
	public static void main(String[] args) throws Exception{

		Job job = new Job(new Configuration(), "Main");

		job.setJarByClass(Main.class);
		
		job.setMapperClass(MapperB.class);
		
		job.setReducerClass(ReducerB.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//senza di questo non funge
		job.setMapOutputKeyClass(Bigramma.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);

}
	
}
