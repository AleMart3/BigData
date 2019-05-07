package job1;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utilsJob1.FirstReducerOutputValues;
import utilsJob1.SecondReducerOutputValues;
import utilsJob1.TickerDate;


public class Main {
	
	public static void main(String[] args) throws Exception {
		
		Runtime.getRuntime().exec("rm -r "+System.getProperty("user.dir")+"/output_job1");

		long start = System.currentTimeMillis();
	
		Job job = new Job(new Configuration(), "Main");

		job.setJarByClass(Main.class);
		
		job.setMapperClass(FirstMapper.class);
				
		job.setReducerClass(FirstReducer.class);

		FileInputFormat.addInputPath(job, new Path("input/historical_stock_prices.csv"));
		FileOutputFormat.setOutputPath(job, new Path("output_job1/firstMapReduce"));
		
		job.setMapOutputKeyClass(TickerDate.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FirstReducerOutputValues.class);

		job.waitForCompletion(true);
		
		//-------------------- job2---------
		
		Job job2 = new Job(new Configuration(), "Main2");

		job2.setJarByClass(Main.class);
		
		job2.setMapperClass(SecondMapper.class);
				
		job2.setReducerClass(SecondReducer.class);

		FileInputFormat.addInputPath(job2, new Path("output_job1/firstMapReduce"));
		FileOutputFormat.setOutputPath(job2, new Path("output_job1/secondMapReduce"));
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(FirstReducerOutputValues.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(SecondReducerOutputValues.class);

		job2.waitForCompletion(true);
		
		
        long end = System.currentTimeMillis();

    	NumberFormat formatter = new DecimalFormat("#0.000");
    	System.out.print("Execution time is " + formatter.format((end - start) / 1000d/60) + " min");
	}

}
