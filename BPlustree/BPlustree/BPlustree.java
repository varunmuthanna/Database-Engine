package BPlustree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import FilesOperation.*;
import contants.Config;
import contants.DataTypes;

public class BPlustree {

	public TableFile file;
	static int pagesize = new Config().PAGE_SIZE;
	
	public BPlustree(TableFile file){
		this.file = file;
	}
	
	public boolean interiorcellposs(int pageno) throws IOException{
		int pos = (pageno) * pagesize;
		pos += 1;
		
		file.seek(pos);
		Byte num_cells = file.readByte();
		if(num_cells < ((pagesize-8)/10)){
			return true;
		}else{
			return false;
		}
	}

	public void addNewCell(int pageno,Integer leafpageno,Integer rowid) throws IOException{
		int pos = pageno * pagesize;
		file.seek(pos);
		//byte pagetype = file.readByte();
		
		//if(pagetype != 0x05){
			String[] colvalue = new String[2];
			colvalue[1] = rowid.toString();
			colvalue[0] = leafpageno.toString();
			Page page = new Page(file, pageno,colvalue, 0x05);
			page.writeinterior(rowid,leafpageno);
		//}else if (pagetype == 0x05){
			
		//}
	}
	
	public void deletepattern(String pattern) throws IOException{
		int pos = findFirstLeafNode() * pagesize;
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = file.readInt();
		int patternfound = 0;
		do{
			file.seek(pos+1);
			int num_cells = file.readByte();
			file.seek(pos+4);
			rightptr = file.readInt();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				file.seek(newpos+(2*i));
				cellpos = file.readShort();
				file.seek(pos+cellpos+6);
				num_colms = file.readByte();
				String colvalue;
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					coltypepos = pos+cellpos+7+j;
					file.seek(coltypepos);
					Byte type = file.readByte();
					file.seek(colvalpos);
					colvalue = file.readByType(type,1);
					if(colvalue.equals(pattern)){
						patternfound = 1;
					}
					int size = DataTypes.getTypeSize(DataTypes.getCodeval(type));
					if(size == 0){
						colvalpos += colvalue.length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1){
					file.seek(pos+cellpos);
					int cellsize = file.readShort()+6;
					for(int k = 0; k < cellsize; k++){
						file.seek(pos+cellpos+k);
						file.writeByte(0x00);
					}
					file.seek(newpos+(2*i));
					file.writeShort(0);
					if(i == num_cells-1){
						file.seek(pos+1);
						file.writeByte(num_cells-1);
					}
				}
			}
			pos = (rightptr == -1) ? pos :rightptr*pagesize;
		}while(rightptr != -1);
	}
	
	
	public void deletepattern(String pattern, int ordinalnum, String oper) throws IOException{
		int pos = findFirstLeafNode() * pagesize;
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = file.readInt();
		int patternfound = 0;
		int ordnumcheck = 0;
		do{
			file.seek(pos+1);
			int num_cells = file.readByte();
			file.seek(pos+4);
			rightptr = file.readInt();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				ordnumcheck = 1;
				file.seek(newpos+(2*i));
				cellpos = file.readShort();
				if(cellpos == 0){
					continue;
				}
				file.seek(pos+cellpos+6);
				num_colms = file.readByte();
				file.seek(pos+cellpos+2);
				Integer rowid = file.readInt();
				String colvalue = rowid.toString();
				if(ordnumcheck == ordinalnum){
					if(file.comparetovalue((byte)0x06, colvalue, pattern, oper)){
						patternfound = 1;
					}
				}
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					ordnumcheck++;
					coltypepos = pos+cellpos+7+j;
					file.seek(coltypepos);
					Byte type = file.readByte();
					file.seek(colvalpos);
					colvalue = file.readByType(type,1);
					if(ordnumcheck == ordinalnum){
						if(file.comparetovalue(type, colvalue, pattern, oper)){
							patternfound = 1;
						}
					}
					int size = DataTypes.getTypeSize(DataTypes.getCodeval(type));
					if(size == 0){
						colvalpos += colvalue.length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1){
					file.seek(pos+cellpos);
					int cellsize = file.readShort()+6;
					for(int k = 0; k < cellsize; k++){
						file.seek(pos+cellpos+k);
						file.writeByte(0x00);
					}
					file.seek(newpos+(2*i));
					file.writeShort(0);
					if(i == num_cells-1){
						file.seek(pos+1);
						file.writeByte(num_cells-1);
					}
				}
			}
			pos = (rightptr == -1) ? pos :rightptr*pagesize;
		}while(rightptr != -1);
	}
	
	public void deletepattern(String pattern, int ordinalnum, String oper,String pattern2,int ordinalnum2, String oper2) throws IOException{
		int pos = findFirstLeafNode() * pagesize;
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = file.readInt();
		int patternfound = 0;
		int pattern2found = 0;
		int ordnumcheck = 0;
		do{
			file.seek(pos+1);
			int num_cells = file.readByte();
			file.seek(pos+4);
			rightptr = file.readInt();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				pattern2found = 0;
				ordnumcheck = 1;
				file.seek(newpos+(2*i));
				cellpos = file.readShort();
				if(cellpos == 0){
					continue;
				}
				file.seek(pos+cellpos+6);
				num_colms = file.readByte();
				file.seek(pos+cellpos+2);
				Integer rowid = file.readInt();
				String colvalue = rowid.toString();
				if(ordnumcheck == ordinalnum){
					if(file.comparetovalue((byte)0x06, colvalue, pattern, oper)){
						patternfound = 1;
					}
				}
				if(ordnumcheck == ordinalnum2){
					if(file.comparetovalue((byte)0x06, colvalue, pattern2, oper2)){
						pattern2found = 1;
					}
				}
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					ordnumcheck++;
					coltypepos = pos+cellpos+7+j;
					file.seek(coltypepos);
					Byte type = file.readByte();
					file.seek(colvalpos);
					colvalue = file.readByType(type,1);
					if(ordnumcheck == ordinalnum){
						if(file.comparetovalue(type, colvalue, pattern, oper)){
							patternfound = 1;
						}
					}
					if(ordnumcheck == ordinalnum2){
						if(file.comparetovalue(type, colvalue, pattern2, oper2)){
							pattern2found = 1;
						}
					}
					int size = DataTypes.getTypeSize(DataTypes.getCodeval(type));
					if(size == 0){
						colvalpos += colvalue.length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1 & pattern2found==1){
					file.seek(pos+cellpos);
					int cellsize = file.readShort()+6;
					for(int k = 0; k < cellsize; k++){
						file.seek(pos+cellpos+k);
						file.writeByte(0x00);
					}
					file.seek(newpos+(2*i));
					file.writeShort(0);
					if(i == num_cells-1){
						file.seek(pos+1);
						file.writeByte(num_cells-1);
					}
				}
			}
			pos = (rightptr == -1) ? pos :rightptr*pagesize;
		}while(rightptr != -1);
	}
	
	public void update(String value, int ordnum, int[] Ordinalnum,String[] values, String[] oper1, String[] oper2) throws IOException{
		int[] found = new int[oper1.length];
		int pos = findFirstLeafNode() * pagesize;
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = file.readInt();
		//List<String[]> list = new ArrayList<String[]>();
		int patternfound = 0;
		int ordnumcheck = 0;
		int conditionMet = 0;
		do{
			file.seek(pos+1);
			int num_cells = file.readByte();
			file.seek(pos+4);
			rightptr = file.readInt();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				ordnumcheck = 1;
				file.seek(newpos+(2*i));
				cellpos = file.readShort();
				if(cellpos == 0){
					continue;
				}
				file.seek(pos+cellpos+6);
				num_colms = file.readByte();
				file.seek(pos+cellpos+2);
				Integer rowid = file.readInt();
				String colvalue = rowid.toString();
				for(int k=0; k < Ordinalnum.length; k++){
					int ordinalnum = Ordinalnum[k];
					if(ordnumcheck == ordinalnum){
						if(file.comparetovalue((byte)0x06, colvalue, values[k], oper1[k])){
							found[k] = 1;
							patternfound = 1;
						}
					}
				}
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					ordnumcheck++;
					coltypepos = pos+cellpos+7+j;
					file.seek(coltypepos);
					Byte type = file.readByte();
					file.seek(colvalpos);
					colvalue = file.readByType(type,1);
					for(int k=0; k < Ordinalnum.length; k++){
						int ordinalnum = Ordinalnum[k];
						if(ordnumcheck == ordinalnum){
							if(file.comparetovalue(type, colvalue, values[k], oper1[k])){
								found[k] = 1;
								patternfound = 1;
							}
						}
					}
					int size = DataTypes.getTypeSize(DataTypes.getCodeval(type));
					if(size == 0){
						colvalpos += colvalue.length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1){
					conditionMet = found[0];
					for(int k=1; k < found.length; k++){
						if(oper2[k-1].equals("and")){
							conditionMet = conditionMet & found[k];
						}else{
							conditionMet = conditionMet | found[k];
						}
					}
					
					if(conditionMet == 1){
						ordnumcheck = 1;
						file.seek(pos+cellpos+6);
						num_colms = file.readByte();
						file.seek(pos+cellpos+2);
						if(ordnum == ordnumcheck){
							file.writeInt(Integer.parseInt(value));
						}
						coltypepos = pos+cellpos+7;
						colvalpos = pos+cellpos+7+num_colms;
						for(int j = 0; j < num_colms; j++){
							ordnumcheck++;
							coltypepos = pos+cellpos+7+j;
							file.seek(coltypepos);
							Byte type = file.readByte();
							file.seek(colvalpos);
							colvalue = file.readByType(type,1);
							//int datalen = 0;
							if(ordnum == ordnumcheck){
								file.seek(colvalpos);
								file.writeByType(value, type);
							}
							int size = DataTypes.getTypeSize(DataTypes.getCodeval(type));
							if(size == 0){
								colvalpos += colvalue.length();
							}else{
								colvalpos += size;
							}
						}
					}
				}
			}
			pos = (rightptr == -1) ? pos :rightptr*pagesize;
		}while(rightptr != -1);
	}
	
	public int findAddLeafForCell(int rowid,String[] coltype, String[] colvalue, int cellsize,int pageno) throws IOException{
		int ret = 0;
		Byte pagetype = 0;
		if(pageno == -1){
			
		}else{
			int pos = (pageno)*pagesize;
			file.seek(pos);
			pagetype = file.readByte();
		}
		if(pagetype == 0){
			int newpageno = (int)file.length()/pagesize;
			file.setLength(file.length()+pagesize);
			Page page = new Page(file,rowid,coltype,colvalue,cellsize,newpageno,(byte)0x0D);
			page.writeleaf();
			return newpageno;
		}else if(pagetype == 0x05){
			Page page = new Page(file);
			int leafnode = page.findleafnode(rowid,pageno,cellsize);
			page = new Page(file,rowid,coltype,colvalue,cellsize,leafnode,(byte)0x0D);
			ret = page.writeleaf();
			if(ret == -1){
				int oldleafnode = leafnode;
				leafnode = findAddLeafForCell(rowid,coltype,colvalue,cellsize,-1);
				int pos = (oldleafnode)*pagesize;
				file.seek(pos+4);
				file.writeInt(leafnode);
			}
			return leafnode;
		}else{
			
		}
		
		return ret;
	}
	
	public int findlastrowid(int pageno) throws IOException{
		int pos = (pageno)*pagesize;
		file.seek(pos);
		Byte pagetype = file.readByte();
		if(pagetype == 0){
			return 0;
		}else if(pagetype == 0x05){
			file.seek(pos+4);
			int rightpointer = file.readInt();
			if(rightpointer != -1){
				return findlastrowid(rightpointer);
			}else{
				file.seek(pos+2);
				short lastcellpos = file.readShort();
				file.seek(pos+lastcellpos);
				int leafpage = file.readInt();
				return findlastrowid(leafpage);
			}
		}else if(pagetype == 0x0D){
			file.seek(pos+1);
			short lastcellpos;
			int num_cells = file.readByte();
			while(true){
				file.seek(pos+8+(num_cells*2));
				lastcellpos = file.readShort();
				if(lastcellpos == 0){
					num_cells--;
				}
				else{
					break;
				}
				if(num_cells == 0){
					return -1;
				}
			}
			/*file.seek(pos+2);
			short lastcellpos = file.readShort();*/
			file.seek(pos+lastcellpos+2);
			return file.readInt();
		}
		
		return -1;
	}
	
	
	public void insert(int rowid,String[] coltype, String[] colvalue, int cellsize,int pageno,boolean leaf) throws IOException{
		if(!leaf){
			if(file.length() < ((pageno+1)*pagesize)){
				file.setLength((pageno+1)*pagesize);
			}

			if(interiorcellposs(pageno)){
				int leafpageno = findAddLeafForCell(rowid,coltype,colvalue,cellsize,pageno);

				addNewCell(pageno,leafpageno,rowid);
			}
		}
	}
	
	public int findFirstLeafNode() throws IOException{
		int pos = 0;
		int leafnode = 0;
		int cellpos = 0;
		while(true){
			file.seek(pos+8);
			cellpos = file.readShort();
			file.seek(pos+cellpos);
			leafnode = file.readInt();
			int newpos = leafnode * pagesize;
			file.seek(newpos);
			int pagetype = file.readByte();
			if(pagetype == 0x0D){
				break;
			}else{
				pos = newpos;
			}
		}
		
		return leafnode;
	}
	
	public List<String[]> findHeader(String table_name) throws IOException{
		int pos = findFirstLeafNode() * pagesize;
		file.seek(pos+4);
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = file.readInt();
		List<String[]> list = new ArrayList<String[]>();
		String colvalue[];
		boolean cellfound = false;
		boolean cellpassed = false;
		do{
			file.seek(pos+1);
			int num_cells = file.readByte();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				file.seek(newpos+(2*i));
				cellpos = file.readShort();
				if(cellpos == 0){
					continue;
				}
				file.seek(pos+cellpos+6);
				num_colms = file.readByte();
				file.seek(pos+cellpos+7);
				int tblnamesize = file.readByte() - 0x0C;
				int tblnamepos = pos+cellpos+7+num_colms;
				file.seek(tblnamepos);
				String tblname = file.readString(tblnamesize);
				if(tblname.equals(table_name)){
					colvalue = new String[num_colms+1];
					file.seek(pos+cellpos+2);
					Integer rowid = file.readInt();
					colvalue[0] = rowid.toString();
					int coltypepos = pos+cellpos+7;
					int colvalpos = pos+cellpos+7+num_colms;
					for(int j = 0; j < num_colms; j++){
						coltypepos = pos+cellpos+7+j;
						file.seek(coltypepos);
						Byte type = file.readByte();
						file.seek(colvalpos);
						colvalue[j+1] = file.readByType(type,1);
						int size = DataTypes.getTypeSize(DataTypes.getCodeval(type));
						if(size == 0){
							colvalpos += colvalue[j+1].length();
						}else{
							colvalpos += size;
						}
					}
					list.add(colvalue);
					cellfound = true;
				}else if(cellfound == true){
					cellpassed = true;
				}
				
			}
			if(cellfound == true && cellpassed == true){
				break;
			}else{
				pos = (rightptr == -1) ? pos :rightptr;
			}
		}while(rightptr != -1);
		
		return list;
		
	}
	
	public List<String[]> findAll() throws IOException{
		int pos = findFirstLeafNode() * pagesize;
		file.seek(pos+4);
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = file.readInt();
		List<String[]> list = new ArrayList<String[]>();
		String colvalue[];
		do{
			file.seek(pos+1);
			int num_cells = file.readByte();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				file.seek(newpos+(2*i));
				cellpos = file.readShort();
				if(cellpos == 0){
					continue;
				}
				file.seek(pos+cellpos+6);
				num_colms = file.readByte();
				colvalue = new String[num_colms+1];
				file.seek(pos+cellpos+2);
				Integer rowid = file.readInt();
				colvalue[0] = rowid.toString();
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					coltypepos = pos+cellpos+7+j;
					file.seek(coltypepos);
					Byte type = file.readByte();
					file.seek(colvalpos);
					colvalue[j+1] = file.readByType(type,1);
					int size = DataTypes.getTypeSize(DataTypes.getCodeval(type));
					if(size == 0){
						colvalpos += colvalue[j+1].length();
					}else{
						colvalpos += size;
					}
				}
				list.add(colvalue);
				
			}
			pos = (rightptr == -1) ? pos :rightptr;
		}while(rightptr != -1);

		return list;
	}
		
	public List<String[]> findCollmnEntry(String pattern) throws IOException{
		int pos = findFirstLeafNode() * pagesize;
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = file.readInt();
		List<String[]> list = new ArrayList<String[]>();
		String colvalue[];
		int patternfound = 0;
		do{
			file.seek(pos+1);
			int num_cells = file.readByte();
			file.seek(pos+4);
			rightptr = file.readInt();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				file.seek(newpos+(2*i));
				cellpos = file.readShort();
				if(cellpos == 0){
					continue;
				}
				file.seek(pos+cellpos+6);
				num_colms = file.readByte();
				colvalue = new String[num_colms+1];
				file.seek(pos+cellpos+2);
				Integer rowid = file.readInt();
				colvalue[0] = rowid.toString();
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					coltypepos = pos+cellpos+7+j;
					file.seek(coltypepos);
					Byte type = file.readByte();
					file.seek(colvalpos);
					colvalue[j+1] = file.readByType(type,1);
					if(colvalue[j+1].equals(pattern)){
						patternfound = 1;
					}
					int size = DataTypes.getTypeSize(DataTypes.getCodeval(type));
					if(size == 0){
						colvalpos += colvalue[j+1].length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1){
					list.add(colvalue);
				}else{
					continue;
				}
				
			}
			pos = (rightptr == -1) ? pos :rightptr*pagesize;
		}while(rightptr != -1);

		return list;
		
	}
	
	public List<String[]> findCollmnEntry(String pattern,int ordinalnum, String oper) throws IOException{
		int pos = findFirstLeafNode() * pagesize;
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = file.readInt();
		List<String[]> list = new ArrayList<String[]>();
		String colvalue[];
		int patternfound = 0;
		int ordnumcheck = 0;
		do{
			file.seek(pos+1);
			int num_cells = file.readByte();
			file.seek(pos+4);
			rightptr = file.readInt();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				ordnumcheck = 1;
				file.seek(newpos+(2*i));
				cellpos = file.readShort();
				if(cellpos == 0){
					continue;
				}
				file.seek(pos+cellpos+6);
				num_colms = file.readByte();
				colvalue = new String[num_colms+1];
				file.seek(pos+cellpos+2);
				Integer rowid = file.readInt();
				colvalue[0] = rowid.toString();
				if(ordnumcheck == ordinalnum){
					if(file.comparetovalue((byte)0x06, colvalue[0], pattern, oper)){
						patternfound = 1;
					}
				}
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					ordnumcheck++;
					coltypepos = pos+cellpos+7+j;
					file.seek(coltypepos);
					Byte type = file.readByte();
					file.seek(colvalpos);
					colvalue[j+1] = file.readByType(type,1);
					if(ordnumcheck == ordinalnum){
						if(file.comparetovalue(type, colvalue[j+1], pattern, oper)){
							patternfound = 1;
						}
					}
					int size = DataTypes.getTypeSize(DataTypes.getCodeval(type));
					if(size == 0){
						colvalpos += colvalue[j+1].length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1){
					list.add(colvalue);
				}else{
					continue;
				}
				
			}
			pos = (rightptr == -1) ? pos :rightptr*pagesize;
		}while(rightptr != -1);

		return list;
		
	}
}


