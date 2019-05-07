package job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utilsJob1.FirstReducerOutputValues;
import utilsJob1.TickerDate;

public class FirstReducer extends Reducer<TickerDate, Text, Text, FirstReducerOutputValues> {

	/*queste variabili vengono inizializzate solo una volta
	 *si eseguono queste, poi si ripete in loop il metodo reduce non tutto
	 */

	private Double minLow; 
	private Double maxHigh; 
	private Double sommaVol=0.0;
	private Double sommaChiusura=0.0;


	public void reduce(TickerDate key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int cont = 0; 
		this.sommaChiusura = 0.0; 
		this.sommaVol=0.0;
		
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
 				
			aggiornaMinimo(low); 
			aggiornaMassimo(high); 
			sommaVolumi(volume);
			this.sommaChiusura += close; 
			cont ++;

		}

		FirstReducerOutputValues outputValues = new FirstReducerOutputValues(key.getYear(), new DoubleWritable(this.sommaChiusura/cont),
				new DoubleWritable(minLow),new DoubleWritable(maxHigh), new DoubleWritable(sommaVol), new IntWritable(cont));

		context.write(new Text(key.getTicker()), outputValues);

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
