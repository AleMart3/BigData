package job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<LongWritable, Text, Text, FirstReducerOutputValues> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line = value.toString().split("[,]");
		String ticker= line[Costanti2.ticker].trim();
		
		int year= Integer.parseInt(line[Costanti2.year]);
		double close= Double.parseDouble(line[Costanti2.close]);
		double minLow= Double.parseDouble(line[Costanti2.minLow]);
		double maxHigh=Double.parseDouble(line[Costanti2.maxHigh]);
		double sommaVol= Double.parseDouble(line[Costanti2.sommaVol]);
		int cont= Integer.parseInt(line[Costanti2.cont].trim());
		
		FirstReducerOutputValues fr = new FirstReducerOutputValues(new IntWritable(year), new DoubleWritable(close),
			new DoubleWritable(minLow),new DoubleWritable(maxHigh),new DoubleWritable(sommaVol),new IntWritable(cont));
		
	
		context.write(new Text(ticker), fr);
		
	}
	
	
}