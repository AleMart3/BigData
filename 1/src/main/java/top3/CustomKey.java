package top3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey>{
	
	private Text text;
	private IntWritable num;
	
	public CustomKey() {
	}
	
	public CustomKey(Text text, int num) {
		 this.text = text;
		 this.num = new IntWritable(num);
		 
		 }
	
	

	public Text getText() {
		return text;
	}

	public void setText(Text text1) {
		this.text = text1;
	}

	public IntWritable getNum() {
		return num;
	}

	public void setNum(IntWritable num) {
		this.num = num;
	}

	public void set(Text t, IntWritable num) {
		this.text=t;
		this.num=num;
	}
	
	
	
	@Override
	public int hashCode() {
		return text.hashCode() + num.hashCode();
	}
	
	@Override
	 public String toString() {
	 return this.text.toString() + " " + this.num.toString();
	 }


	@Override
	public boolean equals(Object o){
		CustomKey a= (CustomKey)o;
		return this.text.equals(a.getText()) && this.num.equals(a.getNum());
	}
	
	public int compareTo(CustomKey that) {
		int cmp = this.text.hashCode()-that.text.hashCode();
		if(cmp==0)
			cmp= this.num.hashCode()-that.num.hashCode();
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.text = new Text(in.readUTF());
		this.num = new IntWritable(in.readInt());
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.text.toString());
		out.writeInt(this.num.get());
		
	}

	
	
	
	

}
