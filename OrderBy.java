import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;

public class OrderBy {

	public static ArrayList<String> orderByMethod(ArrayList<String> table, List<OrderByElement> orderBy) {
		ArrayList<String> output = new ArrayList<String>();
		for (int j = orderBy.size() - 1; j >= 0; j--) {
			int index = 0;
			
			for (String s : table.get(0).split("\\|")) {
				if (orderBy.get(j).toString().split(" ")[0].equalsIgnoreCase(s))
					break;
				index++;
			}
			for (String tuple : table) {
				String tupleArray[] = tuple.split("\\|");
				String outputTuple = tupleArray[index] + "|";
				for (int i = 0; i < tupleArray.length; i++) {
					if (i != index) {
						outputTuple = outputTuple + tupleArray[i] + "|";
					}
				}
				output.add(outputTuple);
			}
		}

		if (orderBy.get(0).toString().split(" ").length == 1) {
			Collections.sort(output.subList(2, output.size()));
		} else if (orderBy.get(0).toString().split(" ")[1].equals("desc")
				|| orderBy.get(0).toString().split(" ")[1].equals("DESC")
				|| orderBy.get(0).toString().split(" ")[1].equals("Desc")) {
			Collections.sort(output.subList(2, output.size()), Collections.reverseOrder());
		}

		return output;
	}
}