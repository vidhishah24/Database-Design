import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectItems {

	public static ArrayList<String> projections(List<SelectItem> selectItems, ArrayList<String> table) {
		ArrayList<String> projectedList = new ArrayList<String>();
		
		if (selectItems.get(0).toString().equals("*")) {
			projectedList.addAll(table);
		} else {
			for (String tuple : table) {
				String resultTuple = "";
				String values[] = tuple.split("\\|");

				for (SelectItem Item : selectItems) {
					int tempIndex = 0;

					if (Item.toString().contains(".*")) {
						String temp[] = Item.toString().split("\\.");

						for (String str : table.get(0).split("\\|")) {

							if (temp[0].equalsIgnoreCase(str.split("\\.")[0])) {
								resultTuple = resultTuple + values[tempIndex] + "|";
							}
							tempIndex++;
						}
					} else if (Item.toString().contains(" AS ") || Item.toString().contains(" as ")
							|| Item.toString().contains(" As ")) {
						String temp[] = Item.toString().split(" AS ");
						for (String str : table.get(0).split("\\|")) {
							if (temp[1].equalsIgnoreCase(str.split("\\.")[0])) {
								resultTuple = resultTuple + values[tempIndex] + "|";
								break;
							}
							tempIndex++;
						}
					} else {
						for (String str : table.get(0).split("\\|")) {
							if (Item.toString().equalsIgnoreCase(str)) {

								resultTuple = resultTuple + values[tempIndex] + "|";
								break;
							}
							tempIndex++;
						}
					}
				}
				projectedList.add(resultTuple.substring(0, resultTuple.length() - 1));
			}
		}

		return projectedList;
	}
}