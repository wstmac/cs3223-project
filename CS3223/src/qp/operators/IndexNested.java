/** index nested loop join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;

public class IndexNested extends Join{


    int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    HashIndex rightTable; // HashIndex built from the right table

    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    Vector<Tuple> rightMatches; //matching tuples in right table, for a given lefttuple

    int lcurs;    // Cursor for left side buffer
    int rcurs;    //Cursor for right side matching records
    boolean eosl;  // Whether end of stream (left table) is reached

    public IndexNested(Join jn){
        super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/

    public boolean open(){

        /** select number of tuples per batch **/
        int tuplesize=schema.getTupleSize();
        batchsize = Batch.getPageSize()/tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr =(Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
            
        /** initialize the cursors of input buffers **/
        lcurs = 0;
        eosl = false;
            
        /** Right hand side table is to be loaded into a HashIndex
         ** for the Index Nested Join to perform
         **/
        if(!right.open()){
            return false;
        } else{    
            rightTable = new HashIndex();
            rcurs = 0;
            rightTable.setAttribute(rightindex);
            while((rightpage = right.next()) != null){
                rightTable.load(rightpage);
            }
            rightMatches = new Vector<Tuple>();
        }
        if(left.open())
            return true;
        else
            return false;
    }


    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/
    public Batch next(){
        if(eosl && lcurs ==0){ //left table is fully processed
            close();
            return null;
        }
        
        outbatch = new Batch(batchsize);
     
        while (!outbatch.isFull()){
                         
            if (rcurs != 0) {//add previously found matching tuples from right table
                Tuple currlefttuple = leftbatch.elementAt(lcurs); 
                for (int k = rcurs; k < rightMatches.size(); k++) {
                    Tuple righttuple = rightMatches.elementAt(k);
                    if (currlefttuple.checkJoin(righttuple,leftindex,rightindex)) {
                        Tuple outtuple = currlefttuple.joinWith(rightMatches.elementAt(k));
                        outbatch.add(outtuple);                 
                        if(outbatch.isFull()){
                            if(k == rightMatches.size()-1){//case 1:  all matching tuples aded
                                rcurs=0;
                                lcurs = (lcurs + 1) % leftbatch.size();
                            } else { //case 2: some matching tuples in right table not added 
                                rcurs = k+1;
                            }
                            return outbatch;
                        }
                    }                  
                }
                rcurs = 0;
                lcurs = (lcurs + 1) % leftbatch.size();
            }
            
            
            if(lcurs==0){
                /** new left page is to be fetched**/
                if((leftbatch =(Batch) left.next()) == null){
                    eosl=true;
                    return outbatch;
                }
            }
            
            for (int i = lcurs; i < leftbatch.size(); i++) {
                Tuple lefttuple = leftbatch.elementAt(i);
                rightMatches = rightTable.get(lefttuple.dataAt(leftindex));
                if (rightMatches != null && !rightMatches.isEmpty()) {
                    for (int j = rcurs; j < rightMatches.size(); j++) {
                        Tuple righttuple = rightMatches.elementAt(j);
                        if(lefttuple.checkJoin(righttuple,leftindex,rightindex)) {
                            Tuple outtuple = lefttuple.joinWith(rightMatches.elementAt(j));
                            outbatch.add(outtuple);
                            
                            if(outbatch.isFull()){
                                if(j == rightMatches.size()-1){//case 1: all matching tuples added
                                    lcurs= (i+1) % leftbatch.size();
                                    rcurs=0;
                                } else { //case 3: some matching tuples in right table not added 
                                    lcurs = i;
                                    rcurs = j+1;
                                }
                                return outbatch;
                            }
                        }
                        
                    }                 
                }     
               rcurs = 0;
            }
            lcurs = 0;
        }
        return outbatch;
    }



    /** Close the operator */
    public boolean close(){
        return true;
    }
}