package top3;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ComparatoreValore extends WritableComparator {

	
	protected ComparatoreValore() {
		super(Chiave.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		Chiave c1= (Chiave) o1;
		Chiave c2= (Chiave) o2;
		int cmp=0;
		cmp= -1*(c1.getText2().hashCode()-c2.getText2().hashCode());
		return cmp;
	}

	
		
	}


