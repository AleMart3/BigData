package job2;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utilsJob2.CustomKey;
import utilsJob2.CustomKey2;
import utilsJob2.CustomValues;


public class SecondReducer extends Reducer<CustomKey2, CustomValues, Text, Text> {

	private long inizioAnno;
	private long fineAnno;
	private Map<CustomKey, List<Text>> map = new HashMap<CustomKey, List<Text>>(); 

	public void reduce(CustomKey2 key, Iterable<CustomValues> values, Context context) throws IOException, InterruptedException {


		DoubleWritable firstClose = new DoubleWritable();
		DoubleWritable lastClose = new DoubleWritable(); 

		Double SommaQuotazioni= 0.0;

		Double volumi =0.0;
		int cont = 0; 

		for(CustomValues value: values) {
			volumi+= value.getVolume().get();
			SommaQuotazioni+=value.getClose().get();

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
		
		

		aggiungiElemento(new CustomKey(key.getSettore(), key.getAnno()), 
				new Text(volumi+ ", "+ diff_percent+  ", " +SommaQuotazioni + ", " + cont));



	}

	@Override	
	protected void cleanup(Context context) throws IOException, InterruptedException{

 		double totaleVolumi = 0.0; 
		int contDefinitivo = 0; 
		double incrementoSettore = 0; 
		double sommaQuot = 0.0; 

		for (CustomKey key : map.keySet()){
			 totaleVolumi = 0.0; 
			 contDefinitivo = 0; 
			 incrementoSettore = 0; 
			 sommaQuot = 0.0;
			
			for (Text el : map.get(key)){
				
				String[] split = el.toString().split("[,]");
				double volumi = Double.parseDouble(split[0].trim()); 
				double diff_perc = Double.parseDouble(split[1].trim());
				double sommaQuotazioni = Double.parseDouble(split[2].trim());
				int cont = Integer.parseInt(split[3].trim());
				
				contDefinitivo += cont; 
				totaleVolumi += volumi; 
				incrementoSettore += diff_perc; 
				sommaQuot += sommaQuotazioni;
			}
			
			context.write(key.getSettore(), new Text("Anno: " + key.getAnno() +" Volume complessivo: "+totaleVolumi+ " Incremento Percentuale: "+ incrementoSettore+ "%"+
					" Quotazione media:" + sommaQuot/contDefinitivo));
		}

	}


private void aggiungiElemento(CustomKey key, Text cv ){
	if (this.map.containsKey(key))
		this.map.get(key).add(cv);
	else {
		List<Text> list = new ArrayList<Text>(); 
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




