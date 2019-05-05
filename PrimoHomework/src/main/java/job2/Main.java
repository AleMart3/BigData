package job2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	
	public static void main(String[] args) throws Exception {
		Runtime.getRuntime().exec("rm -r "+System.getProperty("user.dir")+"/output_job2");
		
		Job job = new Job(new Configuration(), "Main");

		job.setJarByClass(Main.class);
		
		MultipleInputs.addInputPath(job, new Path("input/FirstTable"),TextInputFormat.class, MapperFirstTable.class);
		MultipleInputs.addInputPath(job, new Path("input/SecondTable"),TextInputFormat.class, MapperSecondTable.class);
				
				
		job.setReducerClass(ReducerJoin.class);

		
		FileOutputFormat.setOutputPath(job, new Path("output_job2/firstMapReduce"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		}
}
