import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import FilesOperation.TableFile;
//import FilesOperation.cell;
import contants.DataTypes;

public class DavisBaseOperation {
	static TableFile davistable;
	static TableFile daviscolm;
	static String folder_path;
	static String DBname;
	
	public void setfoldername(String name){
		DBname = name;
		folder_path = "data/"+name+"/";
	}
	
	public void initialize() throws IOException{

		try {
			File dir = new File("data/catalog");
			if(dir != null){
				boolean success = dir.mkdirs();
				if(!success){
					//System.out.println("could not create directories");
					//return;
				}
			}
			dir = new File("data/defaultdb");
			if(dir != null){
				boolean success = dir.mkdirs();
				if(!success){
					//System.out.println("could not create directories");
					//return;
				}
				
			}
			setfoldername("defaultdb");
			davistable = new TableFile("data/catalog/davisbase_tables.tbl","rw","davisbase_tables");
			if(davistable.length() == 0){
				davistable.setDBname("catalog");
				davistable.addcell();
			}
			davistable.setDBname("catalog");
			
			daviscolm = new TableFile("data/catalog/davisbase_column.tbl","rw","davisbase_columns");
			if(daviscolm.length() == 0){
				daviscolm.setDBname("catalog");
				daviscolm.addcell();
			}
			daviscolm.setDBname("catalog");
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void usedatabase(String usercommand) throws IOException{
		String[] command = usercommand.split(" ");
		setfoldername(command[2].toLowerCase().trim());
	}
	
	public void createDatabase(String usercommand) throws IOException{
		String[] command = usercommand.split(" ");
		File dir = new File("data/"+command[2].toLowerCase().trim());
		if(dir.exists()){
			System.out.println("Database already exists");
			return;
		}else{
			dir.mkdir();
		}
	}
	
	public void dropDatabase(String usercommand) throws IOException{
		String[] command = usercommand.split(" ");
		File dir = new File("data/"+command[2].toLowerCase().trim());
		if(dir.exists()){
			for(File f:dir.listFiles()){
				String path = f.getAbsolutePath();
				String db = path.split("/")[path.split("/").length-2];
				//String tblname = filename.split(".")[0];
				davistable.delete(db, 3, "=");
				daviscolm.delete(db, 8, "=");
				f.delete();
			}
			dir.delete();
		}
		setfoldername("defaultdb");
	}
	
	public void showDatabase(String usercommand) throws IOException{
		List<String[]> list = davistable.getcolumnvalue(daviscolm,2,"davisbase_tables","=");		
		List<String[]> list2 = davistable.findAll();
		System.out.println(list.get(2)[2]);
		System.out.println("----------");
		HashMap<String,Integer> map = new HashMap<>();
		for(int i = 0; i < list2.size(); i++){
			String[] cell = list2.get(i);
			map.put(cell[2], 1);
		}
		
		/*for(Entry<String,Integer> entry: map.entrySet()){
			//System.out.println(entry.getKey());
		}*/
		File dir = new File("data/");
		if(dir.exists()){
			for(String f:dir.list()){
				System.out.println(f);
			}
		}
	}
	
	public void showTables(String usercommand) throws IOException{
		List<String[]> list = davistable.getcolumnvalue(daviscolm,2,"davisbase_tables","=");		
		List<String[]> list2 = davistable.findAll();
		System.out.print(list.get(1)[2]);
		System.out.print("\t"+list.get(2)[2]);
		System.out.print("\n");
		for(int i = 0; i < list2.size(); i++){
			String[] cell = list2.get(i);
			System.out.print(cell[1]);
			System.out.print("\t"+cell[2]);
			System.out.println("\n");
		}
	}
	
	public void insertIntoTable(String command) throws IOException{
		String tableName;
		String[] data;
		String[] collmlist = {};
		if(command.indexOf("(") == command.lastIndexOf("(")){
			String str1 = command.substring(0,command.toLowerCase().indexOf("values"));
			String str2 = command.substring(command.toLowerCase().indexOf("values")+7,command.length());
			
			String[] str1arr = str1.trim().split(" ");
			tableName = str1arr[2].toLowerCase().trim();
			
			str2 = str2.replaceAll("[()']", "");
			data = str2.split(",");
		}else{
			tableName = command.substring(command.indexOf(")")+1,command.toLowerCase().indexOf("values")-1).trim();
			data = command.substring(command.lastIndexOf("(") + 1, command.lastIndexOf(")")).split(",");
			collmlist = command.substring(command.indexOf("(") + 1, command.indexOf(")")).split(",");
		}
		

		List<String[]> templist = daviscolm.getHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();
		
		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(DBname)){
				list.add(cell);
			}
		}
		
		TableFile tablefile = new TableFile(folder_path+tableName+".tbl","rw",tableName);
		//List<String[]> list = daviscolm.getHeaderdetails(tableName);
		if(list.size() == 0){
			System.out.println("The Table you are inserting is not available");
			tablefile.close();
			return;
		}
		
		String[] coltype = new String[data.length-1];
		String[] colvalue= new String[data.length-1]; 
		int cellsize = 0;
		int rowid = 0;
		
		for(int i = 0; i < list.size(); i++){
			String[] collm = list.get(i);
			if(collmlist.length != 0){
				if(!collm[2].equals(collmlist[i].trim())){
					System.out.println("The column list provided does not match the table columns");
					tablefile.close();
					return;
				}
			}
			if(collm[5].toLowerCase().equals("no") && data[i].trim().toLowerCase().equals("null")){
				System.out.println("Value for "+collm[2]+" cannot be NULL");
				tablefile.close();
				return;
			}
			
			if(i == 0){
				if(data[i].contains("[^0-9]")){
					System.out.println("The primary key has to be Integer");
				}else{
					rowid = Integer.parseInt(data[i].trim());
				}
			}else{
				if(collm[3].equals("float")){
					collm[3] = "double";
				}
				coltype[i-1] = collm[3];
				colvalue[i-1] = data[i].trim();
				if(DataTypes.getTypeSize(collm[3]) != 0){
					cellsize += DataTypes.getTypeSize(collm[3]);
				}else{
					cellsize += data[i].trim().length();
				}
			}
		}
		
		tablefile.insert(rowid,coltype,colvalue,cellsize);
		tablefile.close();
	}
	
	
	public void update(String command) throws IOException{
		String str1 = command.substring(0, command.toLowerCase().indexOf("where"));
		String str2 = command.substring(command.toLowerCase().indexOf("where")+6, command.length());
		
		String[] str1arr = str1.split(" ");
		String[] str2arr = str2.split(" ");
		
		String tableName = str1arr[1];
		String colm = str1arr[3];
		String val = str1arr[5];
		int ordnum = 0;
		
		String[] condcolm = new String[str2arr.length/3];
		String[] oper1 = new String[str2arr.length/3];
		String[] values = new String[str2arr.length/3];
		String[] oper2 = new String[(str2arr.length/3)-1];
		int[] ordinalnum = new int[str2arr.length/3];
		int j = 0;
		for(int i = 0; i < str2arr.length; i+=4){
			condcolm[j] = str2arr[i];
			oper1[j] = str2arr[i+1];
			values[j] = str2arr[i+2];
			if((i+3) < str2arr.length){
				oper2[j] = str2arr[i+3];
			}
			j++;
		}
		String tableFileName = folder_path+tableName+".tbl";
		TableFile file = new TableFile(tableFileName,"rw",tableName);
		
		List<String[]> templist = daviscolm.getHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();
		
		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(DBname)){
				list.add(cell);
			}
		}
		
		int numOrd = 0;
		for(int i = 0; i < list.size(); i++){
			String[] cell = list.get(i);
			String colmhead = cell[2];
			if(colm.equals(colmhead)){
				ordnum = Integer.parseInt(cell[4]);
			}
			for(int k=0; k < condcolm.length; k++){
				if(colmhead.equals(condcolm[k])){
					ordinalnum[k] = Integer.parseInt(cell[4]);
					numOrd++;
				}
			}
		}
		if((numOrd != (str2arr.length/3)) || ordnum == 0){
			System.out.println("column name given is not valid column name");
			file.close();
			return;
		}
		
		file.update(val, ordnum, ordinalnum,values, oper1, oper2);
		file.close();
	}
	
	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 * @throws IOException 
	 */
	public void dropTable(String dropTableString) throws IOException {

		String tablename = dropTableString.toLowerCase().split(" ")[2];
		davistable.delete(tablename,2,"=",DBname,3,"=");
		daviscolm.delete(tablename,2,"=",DBname,8,"=");
		File delfile = new File(folder_path+tablename+".tbl");
		if(delfile.exists()){
			delfile.delete();
		}
		
	}
	
	public void delete(String command) throws IOException {
		String[] commandstr = command.split(" ");
		if(commandstr.length < 8){
			System.out.println("Delete command is not in right format");
			return;
		}
		String tableName = commandstr[3];
		String colmName = commandstr[5];
		String oper = commandstr[6];
		String pattern = commandstr[7];
		String tableFileName = folder_path+tableName+".tbl";
		TableFile file = new TableFile(tableFileName,"rw",tableName);
		int ordinalnum = 0;
		List<String[]> templist = daviscolm.getHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();
		
		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(DBname)){
				list.add(cell);
			}
		}
		for(int i = 0; i < list.size(); i++){
			String[] cell = list.get(i);
			if(colmName.equals(cell[2])){
				ordinalnum= Integer.parseInt(cell[4]);
			}
		}
		if(ordinalnum == 0){
			System.out.println("column name given is not valid column name");
			file.close();
			return;
		}
		file.delete(pattern, ordinalnum, oper);
		
		file.close();
 	}
	
	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 * @throws IOException 
	 */
	public void parseQueryString(String command) throws IOException {
		String[] commandstr = command.split(" ");
		String dispcolm = commandstr[1].toLowerCase().trim();
		String tableName = commandstr[3].toLowerCase().trim();
		String tableFileName = folder_path+tableName+".tbl";
		TableFile file = new TableFile(tableFileName,"rw",tableName);
		List<String[]> templist = daviscolm.getHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();
		
		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(DBname)){
				list.add(cell);
			}
		}
		int ordinalnum = 0;
		List<String[]> list2;
		if(commandstr.length > 4){
			String colmname = commandstr[5].toLowerCase().trim();
			String operator = commandstr[6].toLowerCase().trim();
			String value = commandstr[7].toLowerCase().trim();
			for(int i = 0; i < list.size(); i++){
				String[] cell = list.get(i);
				if(colmname.equals(cell[2])){
					ordinalnum= Integer.parseInt(cell[4]);
				}
			}
			if(ordinalnum == 0){
				System.out.println("column name given is not valid column name");
				file.close();
				return;
			}
			list2 = file.getcolumnvalue(file,ordinalnum,value,operator);
			
		}else{
			list2 = file.findAll();
		}
		
		if(dispcolm.equals("*")){
			for(int i = 0; i < list.size(); i++){
				String[] cell = list.get(i);
				System.out.print(""+cell[2]+"\t");
			}
			System.out.print("\n");
			
			for(int i = 0; i < list2.size(); i++){
				String[] cell = list2.get(i);
				for(int j = 0; j < cell.length; j++){
					System.out.print(""+cell[j]+"\t");
				}
				System.out.print("\n");
				
			}
		}	
		file.close();
		
	}
	
	/**
	 *  Stub method for creating new tables
	 *  @param command is a String of the user input
	 */
	public void parseCreateString(String command) {


		String createTable = "create table";
		String tableName = command.substring(createTable.length(), command.indexOf("(")).trim();
		command = command.substring(command.indexOf("(") + 1, command.lastIndexOf(")"));
		
		/*  Code to create a .tbl file to contain table data */
		try {
			/*  Create RandomAccessFile tableFile in read-write mode.
			 * 
			 */
			
			List<String[]> templist = daviscolm.getHeaderdetails(tableName);
			List<String[]> list = new ArrayList<>();
			
			for(int i = 0; i < templist.size(); i++){
				String[] cell = templist.get(i);
				if(cell[7].equals(DBname)){
					list.add(cell);
				}
			}
			
			if(list.size() > 0){
				System.out.println("Table already exists");
				return;
			}
			String tableFileName = folder_path+tableName+".tbl";
			TableFile file = new TableFile(tableFileName,"rw",tableName);
			
			
			file.setDBname(DBname);
			file.createTable(davistable,daviscolm,command);
			file.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
		
		/*  Code to insert a row in the davisbase_tables table 
		 *  i.e. database catalog meta-data 
		 */
		
		/*  Code to insert rows in the davisbase_columns table  
		 *  for each column in the new table 
		 *  i.e. database catalog meta-data 
		 */
	}

}
