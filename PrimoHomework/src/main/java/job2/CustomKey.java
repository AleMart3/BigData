package job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey>{
	
	private Text settore;
	private Text data;
	
	public CustomKey() {
	}
	
	public CustomKey(Text settore, Text data) {
		 this.settore = settore;
		 this.data = data;
		 }
	
				
	public Text getSettore() {
		return settore;
	}

	public void setSettore(Text settore) {
		this.settore = settore;
	}

	public Text getData() {
		return data;
	}

	public void setData(Text data) {
		this.data = data;
	}
	
	public String getAnno() {
		return this.data.toString().substring(0, 4);
	}
	
	public String getMese() {
		return this.data.toString().substring(5, 7);
	}
	public String getGiorno() {
		return this.data.toString().substring(8);
	}
	

	@Override
	public int hashCode() {
		return settore.hashCode() + data.hashCode();
	}
	
	@Override
	 public String toString() {
	 return this.settore.toString() + " " + this.data.toString();
	 }


	@Override
	public boolean equals(Object o){
		CustomKey a= (CustomKey)o;
		return this.settore.equals(a.getSettore()) && this.getAnno().equals(a.getAnno());
	}
	
	public int compareTo(CustomKey that) {
		int cmp = this.settore.hashCode()-that.settore.hashCode();
		if(cmp==0)
			cmp= getAnno().hashCode()-that.getAnno().hashCode();
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.settore = new Text(in.readUTF());
		this.data = new Text(in.readUTF());
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.settore.toString());
		out.writeUTF(this.data.toString());
	}
	
	
	

}
