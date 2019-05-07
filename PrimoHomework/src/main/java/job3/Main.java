package job3;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import job3.CustomKey;
import job3.CustomValues;
import job3.SecondMapper;
import job3.SecondReducer;
import job3.MapperFirstTable;
import job3.MapperSecondTable;
import job3.ReducerJoin;

public class Main {

	public static void main(String[] args) throws Exception {

		Runtime.getRuntime().exec("rm -r "+System.getProperty("user.dir")+"/output_job3");

		long start = System.currentTimeMillis();
 
		// =================== Job1 ===================
		Job job1 = new Job(new Configuration(), "Main");

		job1.setJarByClass(Main.class);

		MultipleInputs.addInputPath(job1, new Path("input/FirstTable"),TextInputFormat.class, MapperFirstTable.class);
		MultipleInputs.addInputPath(job1, new Path("input/SecondTable"),TextInputFormat.class, MapperSecondTable.class);

		job1.setReducerClass(ReducerJoin.class);


		FileOutputFormat.setOutputPath(job1, new Path("output_job3/firstMapReduce"));

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.waitForCompletion(true);
		
		// =================== Job2 ===================

		Job job2 = new Job(new Configuration(), "Main2");

		job2.setJarByClass(Main.class);
		
		job2.setMapperClass(SecondMapper.class);
				
		job2.setReducerClass(SecondReducer.class);

		FileInputFormat.addInputPath(job2, new Path("output_job3/firstMapReduce"));
		FileOutputFormat.setOutputPath(job2, new Path("output_job3/secondMapReduce"));
		
		job2.setMapOutputKeyClass(CustomKey.class);
		job2.setMapOutputValueClass(CustomValues.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);
           	
    	
    	// =================== Job3 ===================
    	
    	
		Job job3 = new Job(new Configuration(), "Main");

		job3.setJarByClass(Main.class);

		MultipleInputs.addInputPath(job1, new Path("input/FirstTable"),TextInputFormat.class, MapperFirstTable.class);
		MultipleInputs.addInputPath(job1, new Path("input/SecondTable"),TextInputFormat.class, MapperSecondTable.class);

		job1.setReducerClass(ReducerJoin.class);


		FileOutputFormat.setOutputPath(job1, new Path("output_job3/firstMapReduce"));

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.waitForCompletion(true);
        long end = System.currentTimeMillis();

    	NumberFormat formatter = new DecimalFormat("#0.000");
    	System.out.print("Execution time is " + formatter.format((end - start) / 1000d/60) + " min\n");
    	
		
		
	}
}

