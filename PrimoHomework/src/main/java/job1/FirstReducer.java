package job1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<TickerDate, Text, TickerDate, Text> {

	private int minLow; 
	private int maxHigh; 
	private int sommaVol = 0; 

	public void reduce(TickerDate key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int cont = 0; 
		int sommaChiusura = 0; 
		
		for (Text value : values){
			String[] line = value.toString().split("[,]");
			
			int close = Integer.parseInt(line[0]); 
			int low = Integer.parseInt(line[1]);
			int high = Integer.parseInt(line[2]);
			int volume = Integer.parseInt(line[3]); 
			
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
		
		String risultato = String.valueOf((sommaChiusura/cont)) + "," + String.valueOf(this.minLow) + "," + String.valueOf(this.maxHigh) + "," + String.valueOf(this.sommaVol) + "," + String.valueOf(cont);
		context.write(key, new Text(risultato));
		// media chiusura  1998 o 2018 calcolare minimo low, massimo high, somma volume, conteggio
		
	}
	
	private void sommaVolumi(int vol){
		this.sommaVol += vol; 
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
