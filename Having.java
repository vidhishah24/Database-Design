import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

public class Having {

	public static HashMap<String, ArrayList<String>> havingFilter(HashMap<String, ArrayList<String>> Map,
			Expression havingExp) throws NumberFormatException, IOException {
		ArrayList<String> temp = new ArrayList<>();
		if (havingExp instanceof EqualsTo) {

			EqualsTo EqualsToExpression = (EqualsTo) havingExp;
			Expression rightExpression = (EqualsToExpression.getRightExpression());
			String rightVal = rightExpression.toString();
			for (String str : Map.keySet()) {
				if (Double.parseDouble(Map.get(str).get(0)) != Double.parseDouble(rightVal)) {
					temp.add(str);
				}
			}
			for (String s : temp) {
				Map.remove(s);
			}
		}

		if (havingExp instanceof NotEqualsTo) {

			NotEqualsTo NotEqualsToExpression = (NotEqualsTo) havingExp;
			Expression rightExpression = (NotEqualsToExpression.getRightExpression());
			String rightVal = rightExpression.toString();
			for (String str : Map.keySet()) {
				if (Double.parseDouble(Map.get(str).get(0)) == Double.parseDouble(rightVal)) {
					temp.add(str);
				}
			}
			for (String s : temp) {
				Map.remove(s);
			}

		}

		if (havingExp instanceof GreaterThan) {
			GreaterThan greaterThanExpression = (GreaterThan) havingExp;
			Expression rightExpression = (greaterThanExpression.getRightExpression());
			String rightVal = rightExpression.toString();
			
			for (String str : Map.keySet()) {
			
				if (Double.parseDouble(Map.get(str).get(0)) <= Double.parseDouble(rightVal)) {
					temp.add(str);
				}
			}
			for (String s : temp) {
				Map.remove(s);
			}
			
		}

		if (havingExp instanceof GreaterThanEquals) {

			GreaterThanEquals greaterThanEqualsExpression = (GreaterThanEquals) havingExp;
			Expression rightExpression = (greaterThanEqualsExpression.getRightExpression());
			String rightVal = rightExpression.toString();
			for (String str : Map.keySet()) {
				if (Double.parseDouble(Map.get(str).get(0)) < Double.parseDouble(rightVal)) {
					temp.add(str);
				}
			}
			for (String s : temp) {
				Map.remove(s);
			}

		}

		if (havingExp instanceof MinorThan) {

			MinorThan minorThanExpression = (MinorThan) havingExp;
			Expression rightExpression = (minorThanExpression.getRightExpression());
			String rightVal = rightExpression.toString();
			for (String str : Map.keySet()) {
				if (Double.parseDouble(Map.get(str).get(0)) >= Double.parseDouble(rightVal)) {
					temp.add(str);
				}
			}
			for (String s : temp) {
				Map.remove(s);
			}

		}
		if (havingExp instanceof MinorThanEquals) {

			MinorThanEquals minorThanEqualsExpression = (MinorThanEquals) havingExp;
			Expression rightExpression = (minorThanEqualsExpression.getRightExpression());
			String rightVal = rightExpression.toString();
			for (String str : Map.keySet()) {
				if (Double.parseDouble(Map.get(str).get(0)) > Double.parseDouble(rightVal)) {
					temp.add(str);
				}
			}
			for (String s : temp) {
				Map.remove(s);
			}

		}

		return Map;
	}

}
