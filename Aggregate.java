import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Column;

public class Aggregate {

	public static String aggregateNoGroupBy(ArrayList<String> table, String selectItem) throws IOException {
		String output = null;
		String colName = selectItem.toString().substring(selectItem.toString().indexOf("(") + 1,
				selectItem.toString().indexOf(")"));
		int index=0;
		String type="";
		if (!colName.contains("*")) {
			if (colName.contains("DISTINCT") || colName.contains("Distinct") || colName.contains("distinct")) {
				colName = colName.split(" ")[1];
			}

			for (String str : table.get(0).split("\\|")) {
				if (str.equalsIgnoreCase(colName))
					break;
				index++;
			}
			type= table.get(1).split("\\|")[index];
		} else {
			colName = "";
		}
		
		
		Double sum = 0.0;
		String row = table.get(2);
		if (selectItem.toString().contains("SUM") || selectItem.toString().contains("sum")
				|| selectItem.toString().contains("Sum")) {
			for (String tuple : table.subList(2, table.size())) {
				String values[] = tuple.split("\\|");
				if (!values[index].equalsIgnoreCase("NULL")) {
					sum = sum + Double.parseDouble(values[index]);
				}
			}
			output = row+"|"+String.valueOf(sum);
		} else if (selectItem.toString().contains("MIN") || selectItem.toString().contains("min")
				|| selectItem.toString().contains("Min")) {
			if(type.equalsIgnoreCase("varchar")){
				String minvalue=table.get(2).split("\\|")[index];
				for (String tuple : table.subList(2, table.size())) {
					String values[] = tuple.split("\\|");
					if (!values[index].equalsIgnoreCase("NULL")) {
						if (minvalue.compareTo(values[index])>0) {
							row=tuple;
							minvalue =values[index];
						}
					}
				}
				output=row+"|"+minvalue;
			}
			else if(type.equalsIgnoreCase("date")){
				String minvalue=table.get(2).split("\\|")[index];
				for (String tuple : table.subList(2, table.size())) {
					String values[] = tuple.split("\\|");
					if (!values[index].equalsIgnoreCase("NULL")) {
						if(Integer.parseInt(values[index].split("-")[0])<Integer.parseInt(minvalue.split("-")[0])){
							row=tuple;
							minvalue=values[index];
						}else if(Integer.parseInt(values[index].split("-")[0])==Integer.parseInt(minvalue.split("-")[0])){
							if(Integer.parseInt(values[index].split("-")[1])<Integer.parseInt(minvalue.split("-")[1])){
								row=tuple;
								minvalue=values[index];
							}
							else if(Integer.parseInt(values[index].split("-")[1])==Integer.parseInt(minvalue.split("-")[1])){
								if(Integer.parseInt(values[index].split("-")[2])<Integer.parseInt(minvalue.split("-")[2])){
									row=tuple;
									minvalue=values[index];
								}
							}
						}
					}
				}
				output=row+"|"+minvalue;
			}
			else{
			double minvalue = Integer.MAX_VALUE;
			for (String tuple : table.subList(2, table.size())) {
				String values[] = tuple.split("\\|");
				if (!values[index].equalsIgnoreCase("NULL")) {
					if (Double.parseDouble(values[index]) < minvalue) {
						row=tuple;
						minvalue = Double.parseDouble(values[index]);
					}
				}
			}
			output = row="|"+String.valueOf(minvalue);
			}
		} else if (selectItem.toString().contains("MAX") || selectItem.toString().contains("max")
				|| selectItem.toString().contains("Max")) {
			if(type.equalsIgnoreCase("varchar")){
				String maxvalue=table.get(2).split("\\|")[index];
				for (String tuple : table.subList(2, table.size())) {
					String values[] = tuple.split("\\|");
					if (!values[index].equalsIgnoreCase("NULL")) {
						if (maxvalue.compareTo(values[index])<0) {
							row=tuple;
							maxvalue =values[index];
						}
					}
				}
				output=row+"|"+maxvalue;
			}
			else if(type.equalsIgnoreCase("date")){
				String maxvalue=table.get(2).split("\\|")[index];
				for (String tuple : table.subList(2, table.size())) {
					String values[] = tuple.split("\\|");
					if (!values[index].equalsIgnoreCase("NULL")) {
						if(Integer.parseInt(values[index].split("-")[0])>Integer.parseInt(maxvalue.split("-")[0])){
							row=tuple;
							maxvalue=values[index];
						}else if(Integer.parseInt(values[index].split("-")[0])==Integer.parseInt(maxvalue.split("-")[0])){
							if(Integer.parseInt(values[index].split("-")[1])>Integer.parseInt(maxvalue.split("-")[1])){
								row=tuple;
								maxvalue=values[index];
							}
							else if(Integer.parseInt(values[index].split("-")[1])==Integer.parseInt(maxvalue.split("-")[1])){
								if(Integer.parseInt(values[index].split("-")[2])>Integer.parseInt(maxvalue.split("-")[2])){
									row=tuple;
									maxvalue=values[index];
								}
							}
						}
					}
				}
				output=row+"|"+maxvalue;
			}
		else{
			double maxvalue = Integer.MIN_VALUE;
			for (String tuple : table.subList(2, table.size())) {
				String values[] = tuple.split("\\|");
				if (!values[index].equalsIgnoreCase("NULL")) {
					if (Double.parseDouble(values[index]) > maxvalue) {
						row=tuple;
						maxvalue = Double.parseDouble(values[index]);
					}
				}
			}
			output = row+"|"+String.valueOf(maxvalue);
		}
		} else if (selectItem.toString().contains("count") || selectItem.toString().contains("COUNT")
				|| selectItem.toString().contains("Count")) {
			if (selectItem.toString().contains("DISTINCT") || selectItem.toString().contains("distinct")
					|| selectItem.toString().contains("Distinct")) {
				HashMap<String,Integer> countMap=new HashMap<String,Integer>();
				for (String tuple : table.subList(2, table.size())) {
					String values[] = tuple.split("\\|");
					if (!values[index].equalsIgnoreCase("NULL")) {
						if(!(countMap.containsKey(values[index]))){
							countMap.put(values[index],1);
						}
					}
				}
				output=row+"|"+String.valueOf(countMap.size());
			}else if(selectItem.toString().equalsIgnoreCase("count(*)")){
				output=row+"|"+String.valueOf(table.size()-2);
			}
	else{
			int count = 0;
			for (String tuple : table.subList(2, table.size())) {
				String values[]=tuple.split("\\|");
				if(!(values[index].equalsIgnoreCase("NULL"))){
					count++;
			}
			}
			output = row+"|"+String.valueOf(count);
				}
		} else if (selectItem.toString().contains("avg") || selectItem.toString().contains("AVG")
				|| selectItem.toString().contains("Avg")) {
			int count = 0;
			for (String tuple : table.subList(2, table.size())) {
				String values[] = tuple.split("\\|");
				if (!values[index].equalsIgnoreCase("NULL")) {
					sum = sum + Double.parseDouble(values[index]);
					count++;
				}
			}
			Double avg = sum / count;
			output = row+"|"+String.valueOf(avg);
		}
		
		return output;
	}

	public static HashMap<String, ArrayList<String>> aggregateGroupBy(ArrayList<String> table, String selectItem,
			List<Column> groupByItems) throws IOException {
		int index = 0;
		String colName = selectItem.toString().substring(selectItem.toString().indexOf("(") + 1,
				selectItem.toString().indexOf(")"));
		String type="";
		if (!colName.contains("*")) {
			if(selectItem.contains("count") || selectItem.contains("COUNT") || selectItem.contains("Count") || !(colName.contains("DISTINCT")) || !(colName.contains("Distinct")) || !(colName.contains("distinct"))){
				colName="";
			}
			else {if (colName.contains("DISTINCT") || colName.contains("Distinct") || colName.contains("distinct")) {
				colName = colName.split(" ")[1];
			}

			for (String str : table.get(0).split("\\|")) {
				if (str.equalsIgnoreCase(colName))
					break;
				index++;
			}
			type= table.get(1).split("\\|")[index];
			}
		} else {
			colName = "";
		}
		HashMap<String, ArrayList<String>> Map = new HashMap<String, ArrayList<String>>();

		if (selectItem.toString().contains("SUM") || selectItem.toString().contains("sum")
				|| selectItem.toString().contains("Sum")) {
			for (String tuple : table.subList(2, table.size())) {
				String resultTuple = "";
				String values[] = tuple.split("\\|");
				int tempIndex = 0;
				for (Column Item : groupByItems) {
					for (String str : table.get(0).split("\\|")) {
						if (Item.toString().equalsIgnoreCase(str)) {
							resultTuple = resultTuple + values[tempIndex] + "|";
							break;
						}
						tempIndex++;
					}
				}
				if (!values[index].equalsIgnoreCase("NULL")) {
					if (Map.containsKey(resultTuple)) {
						ArrayList<String> tempList = Map.get(resultTuple);
						String temp = tempList.get(1);
						tempList.add(0, String
								.valueOf(Double.parseDouble(tempList.get(0)) + Double.parseDouble(values[index])));
						tempList.add(1, temp);
						Map.put(resultTuple, tempList);
					} else {
						ArrayList<String> tempList = new ArrayList<String>();
						tempList.add(values[index]);
						tempList.add(tuple);
						Map.put(resultTuple, tempList);
					}
				}
			}
		} else if (selectItem.toString().contains("MIN") || selectItem.toString().contains("min")
				|| selectItem.toString().contains("Min")) {
			if(type.equalsIgnoreCase("varchar")){
				for (String tuple : table.subList(2, table.size())) {
					String resultTuple = "";
					String values[] = tuple.split("\\|");
					int tempIndex = 0;
					for (Column Item : groupByItems) {
						for (String str : table.get(0).split("\\|")) {
							if (Item.toString().equalsIgnoreCase(str)) {
								resultTuple = resultTuple + values[tempIndex] + "|";
								break;
							}
							tempIndex++;
						}
					}
					if (!values[index].equalsIgnoreCase("NULL")) {
						
						if (Map.containsKey(resultTuple)) {
							if ((Map.get(resultTuple).get(0)).compareTo(values[index])>0) {
								ArrayList<String> tempList = Map.get(resultTuple);
								tempList.set(0, values[index]);
								tempList.set(1, tuple);
								Map.put(resultTuple, tempList);
							} }else {
								ArrayList<String> tempList = new ArrayList<String>();
								tempList.add(values[index]);
								tempList.add(tuple);
								Map.put(resultTuple, tempList);
							}
					}
				}
			}
			else if (type.equalsIgnoreCase("date")) {
				for (String tuple : table.subList(2, table.size())){
					String resultTuple = "";
					String values[] = tuple.split("\\|");
					int tempIndex = 0;
					for (Column Item : groupByItems) {
						for (String str : table.get(0).split("\\|")) {
							if (Item.toString().equalsIgnoreCase(str)) {
								resultTuple = resultTuple + values[tempIndex] + "|";
								break;
							}
							tempIndex++;

						}
					}
					
					if (!values[index].equalsIgnoreCase("NULL")) {
						if (Map.containsKey(resultTuple)) {
						String splitadte[]=values[index].split("-");
							if (Integer.parseInt(Map.get(resultTuple).get(0).split("-")[0]) > Integer.parseInt(splitadte[0])) {
								ArrayList<String> tempList = Map.get(resultTuple);
								String temp = tempList.get(1);
								tempList.add(0, values[index]);
								tempList.add(1, temp);
								Map.put(resultTuple, tempList);
							} else if(Integer.parseInt(Map.get(resultTuple).get(0).split("-")[0]) == Integer.parseInt(splitadte[0])){
								if (Integer.parseInt(Map.get(resultTuple).get(0).split("-")[1]) > Integer.parseInt(splitadte[1])){
									ArrayList<String> tempList = Map.get(resultTuple);
									String temp = tempList.get(1);
									tempList.add(0, values[index]);
									tempList.add(1, temp);
									Map.put(resultTuple, tempList);
									
								}else if (Integer.parseInt(Map.get(resultTuple).get(0).split("-")[1]) == Integer.parseInt(splitadte[1])){
									if (Integer.parseInt(Map.get(resultTuple).get(0).split("-")[2]) > Integer.parseInt(splitadte[2])){
										ArrayList<String> tempList = Map.get(resultTuple);
										String temp = tempList.get(1);
										tempList.add(0, values[index]);
										tempList.add(1, temp);
										Map.put(resultTuple, tempList);
									}
								}
							}
							
						}
						else {
							ArrayList<String> tempList = new ArrayList<String>();
							tempList.add(values[index]);
							tempList.add(tuple);
							Map.put(resultTuple, tempList);
						}
					}
				}
			}
			
			
			else{
			for (String tuple : table.subList(2, table.size())) {
				String resultTuple = "";
				String values[] = tuple.split("\\|");
				int tempIndex = 0;
				for (Column Item : groupByItems) {
					for (String str : table.get(0).split("\\|")) {
						if (Item.toString().equalsIgnoreCase(str)) {
							resultTuple = resultTuple + values[tempIndex] + "|";
							break;
						}
						tempIndex++;
					}
				}
				if (!values[index].equalsIgnoreCase("NULL")) {
					
					if (Map.containsKey(resultTuple)) {
						if (Double.parseDouble(Map.get(resultTuple).get(0)) > Double.parseDouble(values[index])) {
							ArrayList<String> tempList = Map.get(resultTuple);
							tempList.set(0, values[index]);
							tempList.set(1, tuple);
							Map.put(resultTuple, tempList);
						} }else {
							ArrayList<String> tempList = new ArrayList<String>();
							tempList.add(values[index]);
							tempList.add(tuple);
							Map.put(resultTuple, tempList);
						}

				}
			}
			}
			
		} else if (selectItem.toString().contains("MAX") || selectItem.toString().contains("max")
				|| selectItem.toString().contains("Max")) {
			if(type.equalsIgnoreCase("varchar")){
				for (String tuple : table.subList(2, table.size())) {
					String resultTuple = "";
					String values[] = tuple.split("\\|");
					int tempIndex = 0;
					for (Column Item : groupByItems) {
						for (String str : table.get(0).split("\\|")) {
							if (Item.toString().equalsIgnoreCase(str)) {
								resultTuple = resultTuple + values[tempIndex] + "|";
								break;
							}
							tempIndex++;
						}
					}
					if (!values[index].equalsIgnoreCase("NULL")) {
						
						if (Map.containsKey(resultTuple)) {
							if ((Map.get(resultTuple).get(0)).compareTo(values[index])<0) {
								ArrayList<String> tempList = Map.get(resultTuple);
								tempList.set(0, values[index]);
								tempList.set(1, tuple);
								Map.put(resultTuple, tempList);
							} }else {
								ArrayList<String> tempList = new ArrayList<String>();
								tempList.add(values[index]);
								tempList.add(tuple);
								Map.put(resultTuple, tempList);
							}
					}
				}
			}
			else if (type.equalsIgnoreCase("date")) {
				for (String tuple : table.subList(2, table.size())){
					String resultTuple = "";
					String values[] = tuple.split("\\|");
					int tempIndex = 0;
					for (Column Item : groupByItems) {
						for (String str : table.get(0).split("\\|")) {
							if (Item.toString().equalsIgnoreCase(str)) {
								resultTuple = resultTuple + values[tempIndex] + "|";
								break;
							}
							tempIndex++;

						}
					}
					
					if (!values[index].equalsIgnoreCase("NULL")) {
						if (Map.containsKey(resultTuple)) {
						String splitadte[]=values[index].split("-");
							if (Integer.parseInt(Map.get(resultTuple).get(0).split("-")[0]) < Integer.parseInt(splitadte[0])) {
								ArrayList<String> tempList = Map.get(resultTuple);
								String temp = tempList.get(1);
								tempList.add(0, values[index]);
								tempList.add(1, temp);
								Map.put(resultTuple, tempList);
							} else if(Integer.parseInt(Map.get(resultTuple).get(0).split("-")[0]) == Integer.parseInt(splitadte[0])){
								if (Integer.parseInt(Map.get(resultTuple).get(0).split("-")[1]) < Integer.parseInt(splitadte[1])){
									ArrayList<String> tempList = Map.get(resultTuple);
									String temp = tempList.get(1);
									tempList.add(0, values[index]);
									tempList.add(1, temp);
									Map.put(resultTuple, tempList);
									
								}else if (Integer.parseInt(Map.get(resultTuple).get(0).split("-")[1]) == Integer.parseInt(splitadte[1])){
									if (Integer.parseInt(Map.get(resultTuple).get(0).split("-")[2]) < Integer.parseInt(splitadte[2])){
										ArrayList<String> tempList = Map.get(resultTuple);
										String temp = tempList.get(1);
										tempList.add(0, values[index]);
										tempList.add(1, temp);
										Map.put(resultTuple, tempList);
									}
								}
							}
							
						}
						else {
							ArrayList<String> tempList = new ArrayList<String>();
							tempList.add(values[index]);
							tempList.add(tuple);
							Map.put(resultTuple, tempList);
						}
					}
				}
			}
			
			else{
			for (String tuple : table.subList(2, table.size())) {
				String resultTuple = "";
				String values[] = tuple.split("\\|");
				int tempIndex = 0;
				for (Column Item : groupByItems) {
					for (String str : table.get(0).split("\\|")) {
						if (Item.toString().equalsIgnoreCase(str)) {
							resultTuple = resultTuple + values[tempIndex] + "|";
							break;
						}
						tempIndex++;

					}
				}

				if (!values[index].equalsIgnoreCase("NULL")) {
					if (Map.containsKey(resultTuple)) {
						if (Double.parseDouble(Map.get(resultTuple).get(0)) < Double.parseDouble(values[index])) {
							ArrayList<String> tempList = Map.get(resultTuple);
							String temp = tempList.get(1);
							tempList.add(0, values[index]);
							tempList.add(1, temp);
							Map.put(resultTuple, tempList);
						} else {
							ArrayList<String> tempList = new ArrayList<String>();
							tempList.add(values[index]);
							tempList.add(tuple);
							Map.put(resultTuple, tempList);
						}
					}

				}
			}
			}
		} else if (selectItem.toString().contains("COUNT") || selectItem.toString().contains("count")
				|| selectItem.toString().contains("Count")) {

			if (selectItem.toString().contains("DISTINCT") || selectItem.toString().contains("distinct")
					|| selectItem.toString().contains("Distinct")) {

				for (String tuple : table.subList(2, table.size())) {
					String resultTuple = "";
					String values[] = tuple.split("\\|");
					int tempIndex = 0;
					for (Column Item : groupByItems) {
						for (String str : table.get(0).split("\\|")) {
							if (Item.toString().equalsIgnoreCase(str)) {
								resultTuple = resultTuple + values[tempIndex] + "|";
								break;
							}
							tempIndex++;

						}
					}

					if (!values[index].equalsIgnoreCase("NULL")) {
						if (Map.containsKey(resultTuple)) {
							ArrayList<String> tempList = Map.get(resultTuple);
							String tmp = tempList.get(1);
							if (!tempList.contains(values[index])) {
								int temp = Integer.parseInt(tempList.get(0)) + 1;
								tempList.add(0, String.valueOf(temp));
								tempList.add(1, tmp);
								tempList.add(values[index]);
								Map.put(resultTuple, tempList);
							}
						} else {
							ArrayList<String> tempList = new ArrayList<String>();
							tempList.add(0, "1");
							tempList.add(1, tuple);
							tempList.add(2, values[index]);
							Map.put(resultTuple, tempList);
						}

					}

				}
			} else {
				
				for (String tuple : table.subList(2, table.size())) {
					String resultTuple = "";
					String values[] = tuple.split("\\|");
					int tempIndex = 0;
					for (Column Item : groupByItems) {
						for (String str : table.get(0).split("\\|")) {
							if (Item.toString().equalsIgnoreCase(str)) {
								resultTuple = resultTuple + values[tempIndex] + "|";
								break;
							}
							tempIndex++;

						}
					}

					if (Map.containsKey(resultTuple)) {

						ArrayList<String> tempList = Map.get(resultTuple);
						String temp = tempList.get(1);
						tempList.set(0, String.valueOf(Integer.parseInt(Map.get(resultTuple).get(0)) + 1));
						tempList.set(1, temp);
						Map.put(resultTuple, tempList);
					} else {
						ArrayList<String> tempList = new ArrayList<String>();
						tempList.add("1");
						tempList.add(tuple);
						Map.put(resultTuple, tempList);

					}

				}
			}
		} else if (selectItem.toString().contains("AVG") || selectItem.toString().contains("Avg")
				|| selectItem.toString().contains("avg")) {

			for (String tuple : table.subList(2, table.size())) {
				String values[] = tuple.split("\\|");
				String resultTuple = "";
				int tempIndex = 0;
				for (Column Item : groupByItems) {
					for (String str : table.get(0).split("\\|")) {
						if (Item.toString().equalsIgnoreCase(str)) {
							resultTuple = resultTuple + values[tempIndex] + "|";
							break;
						}
						tempIndex++;

					}
				}

				if (!values[index].equalsIgnoreCase("NULL")) {
					if (Map.containsKey(resultTuple)) {
						ArrayList<String> tempList = Map.get(resultTuple);
						String tmp = tempList.get(1);
						String[] temp = tempList.get(0).split(",");
						tempList.set(0, String.valueOf(Double.parseDouble(temp[0]) + Double.parseDouble(values[index]))
								+ "," + String.valueOf(Integer.parseInt(temp[1]) + 1));
						tempList.add(1, tmp);
						Map.put(resultTuple, tempList);

					} else {
						ArrayList<String> tempList = new ArrayList<String>();
						tempList.add(values[index] + "," + "1");

						tempList.add(tuple);
						Map.put(resultTuple, tempList);
					}
				}

			}

			for (String str : Map.keySet()) {
				ArrayList<String> tempList = Map.get(str);
				String row = tempList.get(1);
				String[] temp = tempList.get(0).split(",");
				tempList.set(0, String.valueOf(Double.parseDouble(temp[0]) / Integer.parseInt(temp[1])));
				tempList.set(1, row);
				Map.put(str, tempList);
			}
		}
		return Map;
	}

	public static HashMap<String, String> GroupBy(ArrayList<String> table, List<Column> groupByItems) {

		HashMap<String, String> Map = new HashMap<String, String>();

		for (String tuple : table.subList(2, table.size())) {
			String values[] = tuple.split("\\|");
			String resultTuple = "";
			int tempIndex = 0;
			for (Column Item : groupByItems) {
				for (String str : table.get(0).split("\\|")) {
					if (Item.toString().equalsIgnoreCase(str)) {
						resultTuple = resultTuple + values[tempIndex] + "|";
						break;
					}
					tempIndex++;

				}
			}

			if (!Map.containsKey(resultTuple)) {
				Map.put(resultTuple, tuple);

			}

		}
		return Map;

	}
	
	
	
	
}
