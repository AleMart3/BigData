package meanMAX;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeanMax {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "MeanMax");

		job.setJarByClass(MeanMax.class);
		
		job.setMapperClass(Mappermean.class);
		
		job.setReducerClass(Reducermean.class);

		FileInputFormat.addInputPath(job, new Path("input/meanMax"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}