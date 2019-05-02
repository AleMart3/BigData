package topN1mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaintopN2 {

    public static void main(String[] args) throws Exception {
    	

		Job job = new Job(new Configuration(), "MaintopN2");

		job.setJarByClass(MaintopN2.class);
		
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);

		FileInputFormat.addInputPath(job, new Path("input/topN"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
    	
    	    }

    

   

}