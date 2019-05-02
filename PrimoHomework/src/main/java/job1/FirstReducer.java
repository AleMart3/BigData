package job1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<TickerDate, Text, Text, LongWritable> {

	private int minLow; 
	private int maxHigh; 
	
	public void reduce(TickerDate key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text value : values){
			String[] line = value.toString().split("[,]");
			
			String close = line[0]; 
			String low = line[1];
			String high = line[2];
			String volume = line[3]; 
			
			
		}
		
		// media chiusura  1998 o 2018 calcolare minimo low, massimo high, somma volume, conteggio
		
	}
	
	private void aggiornaMinimo(int low){
		if(this.minLow > low)
			this.minLow = low;
	}
	
	private void aggiornaMassimo(int high){
		if(this.maxHigh < high)
			this.maxHigh = high; 
	}
	
}
