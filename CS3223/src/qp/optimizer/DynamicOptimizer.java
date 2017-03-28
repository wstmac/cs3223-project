package qp.optimizer;

import java.io.*;
import java.util.*;

import qp.operators.*;
import qp.utils.*;

public class DynamicOptimizer {
	static SQLQuery sqlquery;
	int numJoin;
	int numTable;
	/**
	 * vector at position i means all subset s such that |s|=i+1
	 * Set<String> stores the tables name of the corresponding subset plan
	 * Operator is the root of the corresponding subset plan 
	 */
	Vector<Map<Set<String>, Operator>> planSpace;

	public DynamicOptimizer(SQLQuery sqlquery) {
		this.sqlquery = sqlquery;
		this.numTable = this.sqlquery.getFromList().size();
		planSpace = new Vector<Map<Set<String>, Operator>>(this.numTable);
		for (int i = 0; i < this.numTable; i++) {
			Map<Set<String>, Operator> map = new HashMap<Set<String>, Operator>();
			planSpace.add(map);
		}
	}

	public Operator getOptimalPlan() {
		Operator root = null;
	
		// find the plan for subset where |s|=1
		for (int i = 0; i < numTable; i++) {
			Set<String> tableName = new HashSet<String>();
			tableName.add((String) this.sqlquery.getFromList().elementAt(i));
			root = createInitialOperator((String) this.sqlquery.getFromList().elementAt(i));
			planSpace.elementAt(0).put(tableName, root);
		}
		
		// for |s|=2 to N, find the optimal plan
		for (int i = 1; i < numTable; i++) {
			//get all subset s where |s|=i+1
			Map<Integer, Set<String>> allSubset = getAllSubsets(i + 1);
			//for current i, get the best plan for the corresponding subset s
			for (int j = 0; j < allSubset.size(); j++) {
				Set<String> curSubset = allSubset.get(j);
				String[] curSubsetArray = curSubset.toArray(new String[0]);
				Operator curRoot = getBestPlan(curSubsetArray);
				Set<String> tableNames = new HashSet<String>();
				for (int k = 0; k <= i; k++) {
					tableNames.add(curSubsetArray[k]);
				}
				planSpace.elementAt(i).put(tableNames, curRoot);
			}
		}
		return createProjectOp(getLastRoot());
	}
	
	private Operator getLastRoot(){
		Set<String> tables = new HashSet<String>();
		for (int i = 0; i < this.sqlquery.getFromList().size(); i++) {
			tables.add((String) this.sqlquery.getFromList().elementAt(i));
		}
		return planSpace.elementAt(numTable - 1).get(tables);
	}
	
	private Operator createProjectOp(Operator root){
		Vector<Attribute> projectlist = (Vector<Attribute>) sqlquery.getProjectList();
		Operator newRoot = null;
		if (projectlist != null && !projectlist.isEmpty()) {
		    newRoot = new Project(root, projectlist, OpType.PROJECT);
		    Schema newSchema = root.getSchema().subSchema(projectlist);
		    newRoot.setSchema(newSchema);
		}
		if (newRoot != null) {
			//System.out.println("inside new root");
			return newRoot;
		} else {
			//System.out.println("notinside new root");
			return root;
		}
	}
	
	/** AFter finding a choice of method for each operator
	prepare an execution plan by replacing the methods with
	corresponding join operator implementation
		**/
	
	public static Operator makeExecPlan(Operator node) {
	
		if (node.getOpType() == OpType.JOIN) {
			Operator left = makeExecPlan(((Join) node).getLeft());
			Operator right = makeExecPlan(((Join) node).getRight());
			int joinType = ((Join) node).getJoinType();
			int numbuff = BufferManager.getBuffersPerJoin();
			switch (joinType) {
			case JoinType.NESTEDJOIN:
	
				NestedJoin nj = new NestedJoin((Join) node);
				nj.setLeft(left);
				nj.setRight(right);
				nj.setNumBuff(numbuff);
				return nj;
	
			case JoinType.BLOCKNESTED:
	
				// NestedJoin bj2 = new NestedJoin((Join) node);
				BlockNested bj = new BlockNested((Join) node);
				bj.setLeft(left);
				bj.setRight(right);
				bj.setNumBuff(numbuff);
				/* + other code */
				return bj;
				
			case JoinType.INDEXNESTED:
				
				IndexNested in = new IndexNested((Join) node);
				in.setLeft(left);
				in.setRight(right);
				in.setNumBuff(numbuff);
				return in;
	
			case JoinType.SORTMERGE:
	
				NestedJoin sm = new NestedJoin((Join) node);
				/* + other code */
				return sm;
	
			case JoinType.HASHJOIN:
	
				NestedJoin hj = new NestedJoin((Join) node);
				hj.setLeft(left);
				hj.setRight(right);
				hj.setNumBuff(numbuff);
				return hj;
			default:
				return node;
			}
		} else if (node.getOpType() == OpType.SELECT) {
			Operator base = makeExecPlan(((Select) node).getBase());
			((Select) node).setBase(base);
			return node;
		} else if (node.getOpType() == OpType.PROJECT) {
			Operator base = makeExecPlan(((Project) node).getBase());
			((Project) node).setBase(base);
			return node;
		} else {
			return node;
		}
	}
	
	//create scan and select operator for subset s where |s|=1
	private Operator createInitialOperator(String tableName) {
		// create scan operator
		Operator root = null;
		Scan op1 = new Scan(tableName, OpType.SCAN);
		Scan tempop = op1;
		String filename = tableName + ".md";
		try {
			ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
			Schema schm = (Schema) _if.readObject();
			op1.setSchema(schm);
			_if.close();
		} catch (Exception e) {
			System.err.println("createInitialOperator(): Error in reading Schema of table " + filename);
			System.exit(1);
		}
		root = op1;
	
		// create select operator
		Select op2 = null;
		for (int j = 0; j < this.sqlquery.getSelectionList().size(); j++) {
			//System.out.println("inside select op");
			Condition cn = (Condition) this.sqlquery.getSelectionList().elementAt(j);
			if (cn.getOpType() == Condition.SELECT) {
				//System.out.println("table name is: "+tabName+" cnLhsName is: "+cn.getLhs().getTabName());
				if (tableName.equals(cn.getLhs().getTabName())) {
					System.out.println("create select op");
					op2 = new Select(op1, cn, OpType.SELECT);
					op2.setSchema(tempop.getSchema());
					root = op2;
				}
			}
		}
		return root;
	}
	
	// / find the best plan for one subset
	private Operator getBestPlan(String[] subset) {
		int size = subset.length;
		int bestPlanCost = Integer.MAX_VALUE;
		Operator curRoot = null;
		
		//for each table Ti in subset S, compute the cost of Ti join with S-Ti; get the smallest cost
		for (int i = 0; i < size; i++) {

			String tableName = subset[i];
			Set<String> oneTable = new HashSet<String>();
			oneTable.add(tableName);
	
			Set<String> restTables = new HashSet<String>();
			Condition condition = null;
			for (int j = 0; j < size; j++) {
				if (j != i)
					restTables.add(subset[j]);
			}
			Operator preRoot = planSpace.elementAt(size - 2).get(restTables);
			if (preRoot == null) {
				continue;
			}

			condition = getJoinCondition(preRoot, oneTable);
			int plancost;
			PlanCost pc = new PlanCost();
			if (condition != null) {
				Join newJoin = new Join(preRoot, planSpace.elementAt(0).get(oneTable), condition, OpType.JOIN);
				Schema newSchema = preRoot.getSchema().joinWith(planSpace.elementAt(0).get(oneTable).getSchema());
				newJoin.setSchema(newSchema);

	
				int curCost = 0;
				plancost = Integer.MAX_VALUE;
				int type=1;
	
				newJoin.setJoinType(JoinType.BLOCKNESTED);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.BLOCKNESTED;
				}
	
				newJoin.setJoinType(JoinType.NESTEDJOIN);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.NESTEDJOIN;
				}
	
				newJoin.setJoinType(JoinType.INDEXNESTED);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.INDEXNESTED;
				}

				 newJoin.setJoinType(JoinType.HASHJOIN);
				 pc = new PlanCost();
				 curCost = pc.getCost(newJoin);
				 if (curCost < plancost) {
				 	plancost = curCost;
				 	type = JoinType.HASHJOIN;
				 }
	
//				newJoin.setJoinType(JoinType.SORTMERGE);
//				pc = new PlanCost();
//				curCost = pc.getCost(newJoin);
//				if (curCost < plancost) {
//					plancost = curCost;
//					type = JoinType.SORTMERGE;
//				}
	
				newJoin.setJoinType(type);
	
				if (plancost < bestPlanCost) {
					bestPlanCost = plancost;
					curRoot = newJoin;
				}
			}
		}
		System.out.print("subset size " + size + "----for subset[ ");
		for (int t=0; t<subset.length; t++){
			System.out.print(subset[t] + " ");
		}
		System.out.println("]");
		return curRoot;
	}
	
	//get left deep tree
	private Condition getJoinCondition(Operator preRoot, Set<String> oneTable) {
		for (int i = 0; i < this.sqlquery.getJoinList().size(); i++) {
			Condition con = (Condition) this.sqlquery.getJoinList().elementAt(i);
			Attribute leftjoinAttr = con.getLhs();
			Attribute rightjoinAttr = (Attribute) con.getRhs();
			if ((preRoot.getSchema().contains(leftjoinAttr) && (planSpace.elementAt(0).get(oneTable).getSchema().contains(rightjoinAttr)))) {
				return (Condition) con.clone();
			} else if (preRoot.getSchema().contains(rightjoinAttr) && planSpace.elementAt(0).get(oneTable).getSchema().contains(leftjoinAttr)) {
				Condition newCon = (Condition) con.clone();
				newCon.setRhs((Attribute) leftjoinAttr.clone());
				newCon.setLhs((Attribute) rightjoinAttr.clone());
				return newCon;
			}
		}
		return null;
	}
	
	//find all subset sub where |sub|=size, find all possible combination of subset where |sub|=size
	/**
	 * For a given subset S and |S|=Size, it has 2^Size subset, including empty set and itself. Can be also represented by binary representation.
	 * For the binary representation, there are 00000...00 total Size bit. If at one position the number is 0, then means we do not use that table;
	 * if is one, means we will use that table.
	 * Thus, if we want to find all possible subset sub where |sub|=size, we just need to find all possible binary combination, where the total ones equals to size.
	 * This is the basic idea of find all possible subset of size "size".
	 * @param size
	 * @return all possible subset
	 */
	private Map<Integer, Set<String>> getAllSubsets(int size) {
		Map<Integer, Set<String>> map = new HashMap<Integer, Set<String>>();
		int i, j;
		int totalNumOfSubset = (int) Math.pow(2, this.numTable);
		int key = 0;
		for (i = 0; i < totalNumOfSubset; i++) {
			if (size == countNumOfOnes(i)) {//one possible subset
				Set<String> set = new HashSet<String>();
				for (j = 0; j < this.numTable; j++) {//find the corresponding table name and add into the hashset
					if ((i & (1 << j)) != 0)
						set.add((String) sqlquery.getFromList().get(j));
				}
				map.put(key, set);
				key++;
			}
		}
		return map;
	}

	//count number of ones in binary representation. eg: integer 5 is 101 so the total number of ones is 2.
	private int countNumOfOnes(int i) {
		int totalNumOfOnes = 0;
		while (i != 0) {
			if ((i & 1) != 0){
				totalNumOfOnes++;
			}
			i >>= 1;
		}
		return totalNumOfOnes;
	}
}
