package top3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Chiave implements WritableComparable<Chiave>{
	
	private Text text1;
	private Text text2;
	
	public Chiave() {
	}
	
	public Chiave(Text text1, Text text2) {
		 this.text1 = text1;
		 this.text2 = text2;
		 }
	
	

	public Text getText1() {
		return text1;
	}

	public void setText1(Text text1) {
		this.text1 = text1;
	}

	public Text getText2() {
		return text2;
	}

	public void setText2(Text text2) {
		this.text2 = text2;
	}

	public void set(Text t1, Text t2) {
		this.text1=t1;
		this.text2=t2;
	}
	
	
	
	@Override
	public int hashCode() {
		return text1.hashCode() + text2.hashCode();
	}
	
	@Override
	 public String toString() {
	 return this.text1.toString() + " " + this.text2.toString();
	 }


	@Override
	public boolean equals(Object o){
		Chiave a= (Chiave)o;
		return this.text1.equals(a.getText1()) && this.text2.equals(a.getText2());
	}
	
	public int compareTo(Chiave that) {
		int cmp = this.text1.hashCode()-that.text1.hashCode();
		if(cmp==0)
			cmp= this.text2.hashCode()-that.text2.hashCode();
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.text1 = new Text(in.readUTF());
		this.text2 = new Text(in.readUTF());
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.text1.toString());
		out.writeUTF(this.text2.toString());
	}
	
	
	

}
