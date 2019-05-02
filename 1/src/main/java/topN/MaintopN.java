package topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import top3.ComparatoreValore2;
import top3.CustomKey;
import top3.Maintop;
import top3.Mapper2;
import top3.Reducer2;

public class MaintopN {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "MaintopN");

		job.setJarByClass(MaintopN.class);
		
		job.setMapperClass(MappertopN.class);
		job.setReducerClass(ReducertopN.class);

		FileInputFormat.addInputPath(job, new Path("input/topN"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		
//--------------------------------------------------------------------------------
		Job job2 = new Job(new Configuration(),"MaintopN2");
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