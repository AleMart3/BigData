package join;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	
	public static void main(String[] args) throws Exception {
		Runtime.getRuntime().exec("rm -r "+System.getProperty("user.dir")+"/output");
		
		Job job = new Job(new Configuration(), "Main");

		job.setJarByClass(Main.class);
		
		MultipleInputs.addInputPath(job, new Path("input/PrimaTabella"),TextInputFormat.class, MapperFirstTable.class);
		MultipleInputs.addInputPath(job, new Path("input/SecondaTabella"),TextInputFormat.class, MapperSecondTable.class);
				
				
		job.setReducerClass(ReducerJoin.class);

		
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		}
}