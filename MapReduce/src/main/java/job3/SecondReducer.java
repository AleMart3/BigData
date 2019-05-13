package job3;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utilsJob3.CustomKey;
import utilsJob3.CustomKey2;
import utilsJob3.CustomValues;

public class SecondReducer extends Reducer<CustomKey2, CustomValues, Text, Text> {

	private long inizioAnno;
	private long fineAnno;
	private Map<CustomKey, List<IntWritable>> map = new HashMap<CustomKey, List<IntWritable>>(); 

	public void reduce(CustomKey2 key, Iterable<CustomValues> values, Context context) throws IOException, InterruptedException{

		DoubleWritable firstClose = new DoubleWritable();
		DoubleWritable lastClose = new DoubleWritable(); 

		double cont = 0; 

		for(CustomValues value: values) {

			LocalDate data = LocalDate.of(value.getAnno(), value.getMese(), value.getGiorno());
			long time = data.toEpochDay(); //calcola il numero di giorni a partire dal 1970
			if (cont == 0) {
				this.inizioAnno= time;
				this.fineAnno= time;
			}
			cont++;

			aggiornaMinimo(time); 
			aggiornaMassimo(time); 

			if (inizioAnno == time) {
				firstClose.set((double) value.getClose().get());
			}

			if (fineAnno == time) {
				fineAnno = time;
				lastClose.set((double) value.getClose().get());
			}

		}

		double diff_percent = incrementoPercentuale(firstClose.get(),lastClose.get());

		aggiungiElemento(new CustomKey(new Text(key.getNome().toString()), new Text(key.getSettore().toString()), 
				new IntWritable(key.getAnno().get())), new IntWritable((int)diff_percent));
	}

	@Override	
	protected void cleanup(Context context) throws IOException, InterruptedException{

		int diff_percentuale_totale = 0; 

		for (CustomKey key : map.keySet()){
			diff_percentuale_totale = 0; 
			for (IntWritable el : map.get(key)){
				diff_percentuale_totale += el.get();

			}

			context.write(new Text(key.getNome() + ", " + key.getSettore()), new Text(","+key.getAnno() +", "+  (int)diff_percentuale_totale));
		}

	}


	private void aggiungiElemento(CustomKey key, IntWritable cv ){
		if (this.map.containsKey(key))
			this.map.get(key).add(cv);
		else {
			List<IntWritable> list = new ArrayList<IntWritable>(); 
			list.add(cv); 
			this.map.put(key, list);
		}
	}

	private void aggiornaMinimo(long low){
		if(inizioAnno > low)
			inizioAnno = low;
	}

	private void aggiornaMassimo(long high){
		if(fineAnno < high)
			fineAnno = high; 
	}

	private double incrementoPercentuale(double firstClose, double lastClose){
		return ((lastClose/firstClose)*100)-100;
	}

}