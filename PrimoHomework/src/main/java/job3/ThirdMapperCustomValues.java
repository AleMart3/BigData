package job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ThirdMapperCustomValues implements WritableComparable<ThirdMapperCustomValues>{
	
	private Text nome;
	private Text settore;
	
	public ThirdMapperCustomValues() {
	}
	
	public ThirdMapperCustomValues(Text nome, Text settore) {
		 this.nome = nome;
		 this.settore = settore;
		 }
	
	

	public Text getNome() {
		return nome;
	}

	public void setNome(Text nome) {
		this.nome = nome;
	}

	public Text getSettore() {
		return settore;
	}

	public void setSettore(Text settore) {
		this.settore = settore;
	}

	public void set(Text t1, Text t2) {
		this.nome=t1;
		this.settore=t2;
	}
	
	
	
	@Override
	public int hashCode() {
		return nome.hashCode() + settore.hashCode();
	}
	
	@Override
	 public String toString() {
	 return this.nome.toString() + " " + this.settore.toString();
	 }


	@Override
	public boolean equals(Object o){
		ThirdMapperCustomValues a= (ThirdMapperCustomValues)o;
		return this.nome.equals(a.getNome()) && this.settore.equals(a.getSettore());
	}
	
	public int compareTo(ThirdMapperCustomValues that) {
		int cmp = this.nome.hashCode()-that.nome.hashCode();
		if(cmp==0)
			cmp= this.settore.hashCode()-that.settore.hashCode();
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.nome = new Text(in.readUTF());
		this.settore = new Text(in.readUTF());
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.nome.toString());
		out.writeUTF(this.settore.toString());
	}
	
	
	

}
