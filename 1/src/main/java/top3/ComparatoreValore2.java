package top3;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ComparatoreValore2 extends WritableComparator {

	
	protected ComparatoreValore2() {
		super(CustomKey.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		CustomKey c1= (CustomKey) o1;
		CustomKey c2= (CustomKey) o2;
		int cmp=0;
		cmp= -1*(c1.getNum().get()-c2.getNum().get());
		return cmp;
	}

	
		
	}


