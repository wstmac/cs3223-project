/** block nested loops join algorithm **/

package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class BlockNested extends Join{
    int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the BlockNested operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String rfname;    // The file name where the right table is materialize

    static int filenum=0;   // To get unique filenum for this operation

    boolean rightMaterialised = false;
    
    Batch outbatch;   // Output buffer
    Vector<Batch> leftblock;  // Blocks of buffers for left input stream
    Batch rightbatch;  // Buffer for right input stream
    int outblockSize; // number of buffers allocated to outer block
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lbuffer; // Cursor for left side block
    int lcurs;    // Cursor for current left side buffer
    int rcurs;    // Cursor for right side buffer
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)
    
   
    public BlockNested(Join jn){
        super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /** During open finds the index of the join attributes
     ** Materializes the right hand side into a file
     ** Opens the connections
     **/
    
    public boolean open() {
        
        /** select number of tuples per batch **/
        int tuplesize=schema.getTupleSize();
        batchsize = Batch.getPageSize()/tuplesize;
        
        /** select number of buffers per outerblock **/
        outblockSize = numBuff - 2;
        leftblock = new Vector<Batch>(outblockSize);
        
        Attribute leftattr = con.getLhs();
        Attribute rightattr =(Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
        
        /** initialize the cursors of input buffers **/
        lcurs = 0; 
        rcurs = 0;
        eosl = false;
        
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;
        
        /** Right hand side table is to be materialized
         ** for the Block Nested join to perform
         **/
        if(!right.open()){
            return false;
        } else{
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            if(right.getOpType() != OpType.SCAN){
                rightMaterialised = true;
                filenum++;
                rfname = "BNtemp-" + String.valueOf(filenum);
                try{
                    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                    while( (rightpage = right.next()) != null){
                        out.writeObject(rightpage);
                    }
                    out.close();
                } catch(IOException io){
                    System.out.println("BlockNested: writing the temporay file error");
                    return false;
                }
                
                if(!right.close())
                return false;
            }
        }
        if(left.open())
            return true;
        else
            return false;
        
    }
    
    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/
    public Batch next() {
        int i, j, k;
        if(eosl && lbuffer == 0 && lcurs == 0 && eosr){ //both left and right tables have been fully processed
            close();
            return null;
        }
        outbatch = new Batch(batchsize);
        
        while (!outbatch.isFull()) {
            
            if (lbuffer==0 && lcurs == 0 && eosr) {//the current left block and inner right buffer has been fully processed
                /** new left block is to be fetched **/
                
                leftblock.removeAllElements(); // remove current content in buffer
                while (leftblock.size() < outblockSize) { 
                    Batch toAdd = (Batch) left.next();
                    if (toAdd == null || toAdd.isEmpty()) {
                        eosl = true;
                        break;
                    }
                    leftblock.add(toAdd);
                }
                
                if (leftblock.isEmpty()) {
                    eosl = true;
                    return outbatch;
                }
                
                /** Whenver a new left block came , we have to start the
                 ** scanning of right table
                 ** if the right table is not a base table, read from temp file
                 **/
                if (rightMaterialised) {
                    try {               
                        in = new ObjectInputStream(new FileInputStream(rfname));
                    } catch(IOException io){
                        System.err.println(io.getMessage());
                        System.err.println("BlockNestedJoin:error in reading the file");
                        System.exit(1);
                    }
                } else {
                    right.open();
                }
                eosr = false;    
                rcurs = 0;
            }      
            
            while (eosr == false) {
                try {
                    //read in a new right batch when the previous right batch has been checked against the current left block
                    if (rcurs == 0 && lbuffer == 0 && lcurs == 0) {
                        if (rightMaterialised) {
                            rightbatch = (Batch) in.readObject();
                        } else {
                            rightbatch = right.next();
                            if (rightbatch == null || rightbatch.isEmpty()) {
                                throw new EOFException("No more tuples in right table");
                            }
                        }
                    }
                    
                    for (i = lbuffer; i < leftblock.size(); i++) {
                        Batch leftbatch = leftblock.elementAt(i);                        
                        for (j = lcurs; j < leftbatch.size(); j++) {
                            Tuple lefttuple = leftbatch.elementAt(j);
                            for (k = rcurs; k < rightbatch.size(); k++) {
                                Tuple righttuple = rightbatch.elementAt(k);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                                              
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (k != rightbatch.size()-1) {//case 1: inner loop not completed
                                            lbuffer = i;
                                            lcurs = j;
                                            rcurs = k+1;
                                        } else if (j != leftbatch.size()-1){ //case 2: current leftbatch not completed;
                                            lbuffer = i;
                                            lcurs = j+1;
                                            rcurs = 0;
                                        } else if(i != leftblock.size()-1){//case 3: left block have unprocessed buffer
                                            lbuffer = i+1;
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else {// case 4: left block completed
                                            lbuffer = 0;
                                            lcurs = 0;
                                            rcurs = 0;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcurs = 0;
                    }
                    lbuffer = 0;
                } catch(EOFException e){
                    try{
                        if (rightMaterialised) in.close();
                        else right.close();
                    }catch (IOException io){
                    System.out.println("BlockNestedJoin:Error in temporary file reading");
                    }
                    eosr=true;
                } catch(ClassNotFoundException c){
                    System.out.println("BlockNestedJoin:Some error in deserialization ");
                    System.exit(1);
                } catch(IOException io){
                    System.out.println("BlockNestedJoin:temporary file reading error");
                    System.exit(1);
                }
            }            
        }
        return outbatch;
    }
    
    /** Close the operator */
    public boolean close(){
        if (rightMaterialised) {
            File f = new File(rfname);
            f.delete();
        }
        return left.close()&&right.close();
    } 
}
