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


public class SecondReducer extends Reducer<Text, FirstReducerOutputValues, Text, SecondReducerOutputValues> {

	private Map<Text, SecondReducerOutputValues> countMap = new HashMap<Text, SecondReducerOutputValues>();

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
		
		SecondReducerOutputValues outputValues= new SecondReducerOutputValues(differenza, absoluteMin, absoluteMax, 
									new DoubleWritable(sommaVolumi/cont), new DoubleWritable(incrementoPercentuale));

		countMap.put(new Text(key), outputValues);
	}


	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		Map<Text, SecondReducerOutputValues> sortedMap = sortByValues(countMap);

		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			if (counter++ == 10) {
				break;
			}
			context.write(key, sortedMap.get(key));
		}
	}


	/*
	 * sorts the map by values. Taken from:
	 * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
	 */
	@SuppressWarnings("rawtypes")
	private static <K extends Comparable, V extends Comparable> Map<Text, SecondReducerOutputValues> sortByValues(Map<Text, SecondReducerOutputValues> map) {

		//trasforma la mappa in una lista dove ogni elemento della lista è chiave=valore

		List<Map.Entry<Text, SecondReducerOutputValues>> entries = new LinkedList<Map.Entry<Text, SecondReducerOutputValues>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<Text, SecondReducerOutputValues>>() {
			
			public int compare(Map.Entry<Text, SecondReducerOutputValues> o1, Map.Entry<Text, SecondReducerOutputValues> o2) {
				return o2.getValue().getDifferenzaChiusura().compareTo(o1.getValue().getDifferenzaChiusura());
			}
		});

		//LinkedHashMap will keep the keys in the order they are inserted
		Map<Text, SecondReducerOutputValues> sortedMap = new LinkedHashMap<Text, SecondReducerOutputValues>();

		for (Map.Entry<Text, SecondReducerOutputValues> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}




	private double incrementoPercentuale(double close1998, double close2018){
		return ((close2018/close1998)*100)-100;
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