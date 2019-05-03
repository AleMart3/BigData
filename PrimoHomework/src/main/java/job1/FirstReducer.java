package job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<TickerDate, Text, Text, FirstReducerOutputValues> {

	private Double minLow; 
	private Double maxHigh; 
	private Double sommaVol = 0.0; 

	public void reduce(TickerDate key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int cont = 0; 
		Double sommaChiusura = 0.0; 
		
		for (Text value : values){
			String[] line = value.toString().split("[,]");
			
			Double close = Double.parseDouble(line[0]); 
			Double low = Double.parseDouble(line[1]);
			Double high = Double.parseDouble(line[2]);
			Double volume = Double.parseDouble(line[3]); 
			
			if (cont == 0) {
				this.minLow = low;
				this.maxHigh = high;
			}

			if (key.getYear().equals(new IntWritable(1998)) | key.getYear().equals(new IntWritable(2018))){
				sommaChiusura += close; 
			}
			
			aggiornaMinimo(low); 
			aggiornaMassimo(high); 
			sommaVolumi(volume);
			cont ++;
		}
		
		FirstReducerOutputValues outputValues = new FirstReducerOutputValues(key.getYear(), new DoubleWritable(sommaChiusura/cont),
		new DoubleWritable(minLow),new DoubleWritable(maxHigh), new DoubleWritable(sommaVol), new DoubleWritable(cont));
		
		context.write(new Text(key.getTicker()), outputValues);
		
		// media chiusura  1998 o 2018 calcolare minimo low, massimo high, somma volume, conteggio
		
	}
	
	private void sommaVolumi(Double vol){
		this.sommaVol += vol; 
	}
	
	private void aggiornaMinimo(Double low){
		if(this.minLow > low)
			this.minLow = low;
	}
	
	private void aggiornaMassimo(Double high){
		if(this.maxHigh < high)
			this.maxHigh = high; 
	}
	
}
