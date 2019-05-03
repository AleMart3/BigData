package job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper  extends Mapper<LongWritable, Text, Text, FirstReducerOutputValues> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line = value.toString().split("[,]");
		String ticker= line[Costanti2.ticker];
		int year= Integer.parseInt(line[Costanti2.year]);
		Double close= Double.parseDouble(line[Costanti2.close]);
		Double minLow= Double.parseDouble(line[Costanti2.minLow]);
		Double maxHigh=Double.parseDouble(line[Costanti2.maxHigh]);
		Double sommaVol= Double.parseDouble(line[Costanti2.sommaVol]);
		Double cont= Double.parseDouble(line[Costanti2.cont]);
		
		FirstReducerOutputValues fr = new FirstReducerOutputValues(new IntWritable(year), new DoubleWritable(close),
			new DoubleWritable(minLow),new DoubleWritable(maxHigh),new DoubleWritable(sommaVol),new DoubleWritable(cont));
		
		System.out.println(fr);
		context.write(new Text(ticker), fr);
		
		
		
	}
	
	
}