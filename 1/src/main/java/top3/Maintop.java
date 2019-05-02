package top3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Maintop {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "Maintop");

		job.setJarByClass(Maintop.class);
		
		job.setMapperClass(MapperA.class);
		job.setReducerClass(ReducerA.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		
		//---------------------------------------------------------------
		
		Job job2 = new Job(new Configuration(),"Maintop2");
		job2.setJarByClass(Maintop.class);
		
		job2.setMapperClass(Mapper2.class);
		job2.setSortComparatorClass(ComparatoreValore2.class);
		job2.setReducerClass(Reducer2.class);
		
		
		FileInputFormat.addInputPath(job2, new Path("output"));
		FileOutputFormat.setOutputPath(job2, new Path("output2"));
		
		job2.setMapOutputKeyClass(CustomKey.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);
		
		
		
	}
}