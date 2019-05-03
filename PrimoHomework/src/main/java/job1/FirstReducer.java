package job1;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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
		Map<TickerDate, Text> countMap = new HashMap<TickerDate,Text>();
		Map<TickerDate, Text> sortedMap = sortByValues(countMap);

		for (Text value : sortedMap.values()){
			String[] line = value.toString().split("[,]");

			Double close = Double.parseDouble(line[0]); 
			Double low = Double.parseDouble(line[1]);
			Double high = Double.parseDouble(line[2]);
			Double volume = Double.parseDouble(line[3]); 

			if (cont == 0) {
				this.minLow = low;
				this.maxHigh = high;
			}

			if (cont==0 || cont==sortedMap.values().size()-1){
				sommaChiusura = close; 
			}

			aggiornaMinimo(low); 
			aggiornaMassimo(high); 
			sommaVolumi(volume);
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


	/*
	 * sorts the map by values. Taken from:
	 * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
	 */
	@SuppressWarnings("rawtypes")
	private static <K extends Comparable, V extends Comparable> Map<TickerDate, Text> sortByValues(Map<TickerDate, Text> map) {

		//trasforma la mappa in una lista dove ogni elemento della lista è chiave=valore

		List<Map.Entry<TickerDate, Text>> entries = new LinkedList<Map.Entry<TickerDate, Text>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<TickerDate, Text>>() {

			public int compare(Map.Entry<TickerDate, Text> o1, Map.Entry<TickerDate, Text> o2) {
				return o2.getKey().getYear().compareTo(o1.getKey().getYear());
			}
		});

		//LinkedHashMap will keep the keys in the order they are inserted
		Map<TickerDate, Text> sortedMap = new LinkedHashMap<TickerDate, Text>();

		for (Map.Entry<TickerDate, Text> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}


}
