package job2;


import java.io.IOException;
import java.sql.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SecondReducer extends Reducer<CustomKey, CustomValues, CustomKey, Text> {
	
	private long inizioAnno;
	private long fineAnno;

	public void reduce(CustomKey key, Iterable<CustomValues> values, Context context) throws IOException, InterruptedException {

		
		DoubleWritable firstClose = new DoubleWritable();
		DoubleWritable lastClose = new DoubleWritable(); 
		
		Double volumi =0.0;
		int cont = 0; 
	
		for(CustomValues value: values) {
			volumi+= value.getVolume().get();


			@SuppressWarnings("deprecation")
			Date data= new Date(Integer.parseInt(key.getAnno()),Integer.parseInt(key.getMese()),Integer.parseInt(key.getGiorno()));
			long time = data.getTime();
			
			
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
	
		
	
		System.out.println(firstClose);
		System.out.println(lastClose);
	
		System.out.println("--------");

		context.write(key, new Text(String.valueOf(volumi)));


	}
	
	
	private void aggiornaMinimo(long low){
		if(inizioAnno > low)
			inizioAnno = low;
	}

	private void aggiornaMassimo(long high){
		if(fineAnno < high)
			fineAnno = high; 
	}


}