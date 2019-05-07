package job3;

import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utilsJob3.CustomKey;
import utilsJob3.CustomValues;

public class SecondReducer extends Reducer<CustomKey, CustomValues, Text, Text> {

	private long inizioAnno;
	private long fineAnno;

	public void reduce(CustomKey key, Iterable<CustomValues> values, Context context) throws IOException, InterruptedException {

		DoubleWritable firstClose = new DoubleWritable();
		DoubleWritable lastClose = new DoubleWritable(); 
		
		int cont = 0; 

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
				firstClose.set(value.getClose().get());
			}

			if (fineAnno == time) {
				fineAnno = time;
				lastClose.set(value.getClose().get());
			}

		}

		Double diff_percent = incrementoPercentuale(firstClose.get(),lastClose.get());

		context.write(new Text(key.getNome() + ", " + key.getSettore()), new Text(","+key.getAnno() +", "+  diff_percent));

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

