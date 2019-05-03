package job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SecondReducer extends Reducer<Text, FirstReducerOutputValues, Text, FirstReducerOutputValues> {
	private DoubleWritable absoluteMin = new DoubleWritable(); 
	private DoubleWritable absoluteMax = new DoubleWritable(); 
	
	public void reduce(Text key, Iterable<FirstReducerOutputValues> values, Context context) throws IOException, InterruptedException {
		DoubleWritable close1998 = new DoubleWritable();
		DoubleWritable close2018 = new DoubleWritable(); 
		double sommaVolumi = 0;
		int cont = 0; 
		
		for (FirstReducerOutputValues value : values){
			
			if (value.getYear().equals(new IntWritable(2018))) {
				close2018 = value.getMediaChiusura(); 
			}
			
			if (value.getYear().equals(new IntWritable(1998))){
				close1998 = value.getMediaChiusura();
			}
			
			if (cont == 0) {
				this.absoluteMin = value.getMinLow();
				this.absoluteMax = value.getMaxHigh();
			}

			aggiornaMinimo(value.getMinLow()); 
			aggiornaMassimo(value.getMaxHigh()); 
			sommaVolumi += value.getSommaVol().get(); 
			cont += value.getCont().get(); 
		}
		
		DoubleWritable differenza = new DoubleWritable(close2018.get()-close1998.get());
		double incrementoPercentuale = incrementoPercentuale(close1998.get(), differenza.get()); 
		//context.write(key, value);
	}
	
	private double incrementoPercentuale(double close1998, double diff){
		return (diff/close1998)*100;
	}
	
	private void aggiornaMinimo(DoubleWritable low){
		if(this.absoluteMin.get() > low.get())
			this.absoluteMin = low;
	}
	
	private void aggiornaMassimo(DoubleWritable high){
		if(this.absoluteMax.get() < high.get())
			this.absoluteMax = high; 
	}
}