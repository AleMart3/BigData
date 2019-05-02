package callDATA;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainCall {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "callDATA");

		job.setJarByClass(MainCall.class);
		
		job.setMapperClass(MapperCall.class);
		job.setReducerClass(ReducerCall.class);

		FileInputFormat.addInputPath(job, new Path("input/callDATA"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.waitForCompletion(true);
		
	}
}