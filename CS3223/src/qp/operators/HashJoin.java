package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class HashJoin extends Join{

	int batchsize;  //Number of tuples per out batch
	int right_batchsize;
	int left_batchsize;

    /** The following fields are useful during execution of
     ** the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String rfname;    // The file name where the right table is materialize
    String in_lfname;
    String in_rfname;

    static int filenum=-1;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Batch inputbatch;  // Buffer for input stream for left table
    Batch inputbatch_right;
    Batch[] in_memory_ht;//size should be (numBuff -2)
    //Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream in_right; // File pointer to the right hand materialized file
    ObjectInputStream in_left;
    int curr_partition_index = -1;

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    boolean eosl = true;  // Whether end of stream (left table) is reached
    boolean eosr = true;  // End of stream (right table)

    public HashJoin(Join jn){
	super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
	schema = jn.getSchema();
	jointype = jn.getJoinType();
	numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     **  Finish the partition phase: partition right table into numBuff - 1 partitions then left table
     **  
     **/



    public boolean open(){

		/** select number of tuples per batch **/
		int tuplesize=schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;
		int right_tupleSize = right.getSchema().getTupleSize();
		right_batchsize = Batch.getPageSize()/right_tupleSize;//num tuples per batch for right table
		System.out.println("right_batchsize " + right_batchsize);
		int left_tupleSize = right.getSchema().getTupleSize();
		left_batchsize = Batch.getPageSize()/left_tupleSize;//num tuples per batch for left table
		System.out.println("left_batchsize " + left_batchsize);

		//index of join attributes
	Attribute leftattr = con.getLhs();
	Attribute rightattr =(Attribute) con.getRhs();
	leftindex = left.getSchema().indexOf(leftattr);
	rightindex = right.getSchema().indexOf(rightattr);
	
	/** initialize the cursors of input buffers **/
	lcurs = 0; rcurs =0;

	/*input buffer and out put buffer*/
	Batch inputpage;
	
	/*initialise bucket*/
	Batch[] buckets = new Batch[numBuff - 1];
	
	/*partition right table first*/
	if(!right.open()){
	    return false;
	}else{
	    //if(right.getOpType() != OpType.SCAN){
		System.out.println("open right : " + right.getOpType());
	    try{
	    	//initialise bucket
	    	for(int i = 0; i < numBuff-1;i++){
	    		buckets[i] = new Batch(right_batchsize);
	    	}
	    	//initialise right table output filewriter
	    	ObjectOutputStream[] outStream_right = new ObjectOutputStream[numBuff - 1];
	    	for(int i = 0; i<numBuff-1;i++){
	    	    filenum++;
	    	    String fname =  "HJtempRight-" + String.valueOf(filenum) + this.hashCode();
	    	    outStream_right[i] = new ObjectOutputStream(new FileOutputStream(fname));
	    	}
	    	
	    	while( (inputpage = right.next()) != null){
	    		for(int i = 0; i < inputpage.size();i++){
	    			//hash each tuple in input right page
	    			Tuple t = inputpage.elementAt(i);
	    			int key = (t.dataAt(rightindex).hashCode())%(numBuff-1);
	    			//System.out.println("key: " + key);
	    			if(buckets[key].size() == right_batchsize){
	    				//bucket is full, write into disk
	    				outStream_right[key].writeObject(buckets[key]);
	    				buckets[key].clear();
	    			}
	    			buckets[key].add(t);
	    		}
	    	}
	    	//write the rest buckets into file
	    	for(int i = 0; i < numBuff - 1;i++){
	    		if(!buckets[i].isEmpty()){
	    			outStream_right[i].writeObject(buckets[i]);
	    		}
	    	}
	    	
	    	//close output stream
	    	for(int i = 0; i < numBuff -1 ;i++){
	    		outStream_right[i].close();
	    	}
	    }catch(IOException io){
	    	System.out.println("HashJoin:writing the temporay file error for partition phase");
	    	return false;
	    }
		//}
	    if(!right.close())
	    	return false;
	}
	
	/*partition left table*/
	if(!left.open())
	    return false;
	else{
		System.out.println("open left : " + left.getOpType());
	    try{
	    	//initialise bucket
	    	for(int i = 0; i < numBuff-1;i++){
	    		buckets[i] = new Batch(left_batchsize);
	    	}
	    	//initialise left output filewriter
	    	filenum = -1;
	    	ObjectOutputStream[] outStream_left = new ObjectOutputStream[numBuff - 1];
	    	for(int i = 0; i<numBuff-1;i++){
	    		filenum++;
	    		String fname = "HJtempLeft-" + String.valueOf(filenum)+ this.hashCode();
	    		outStream_left[i] = new ObjectOutputStream(new FileOutputStream(fname));
	    	}
    	
	    	while( (inputpage = left.next()) != null){
	    		for(int i = 0; i < inputpage.size();i++){
	    			Tuple t = inputpage.elementAt(i);
	    			int key = (t.dataAt(leftindex).hashCode())%(numBuff-1);
	    			if(buckets[key].size() == left_batchsize){
	    				//bucket is full, write into disk
	    				outStream_left[key].writeObject(buckets[key]);
	    				buckets[key].clear();
	    			}
	    			buckets[key].add(t);
	    		}
	    	}
	    	//write the rest buckets into file
	    	for(int i = 0; i < numBuff - 1;i++){
	    		if(!buckets[i].isEmpty()){
	    			outStream_left[i].writeObject(buckets[i]);
	    		}
	    	}
    	
	    	//close output stream
	    	for(int i = 0; i < numBuff -1 ;i++){
	    		outStream_left[i].close();
	    	}
	    }catch(IOException io){
	    	System.out.println("HashJoin:writing the temporay file error for partition phase");
	    	return false;
	    }
	//}
	    if(!left.close())
	    	return false;
	}  
	return true;
    }



    /** construct in memory hash table for each partition of left table
     * each time fetch one page of right table from same partition
     * hash each tuple and match with the left table tuples in the same bucket
     * selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/


    public Batch next(){
	//System.out.print("HashJoin:--------------------------in next----------------");
	//Debug.PPrint(con);
	//System.out.println();

	outbatch = new Batch(batchsize);
	while(!outbatch.isFull()){
		if(curr_partition_index == -1 ||  eosr == true){
			//if at the beginning or finish matching one round of right table
				if(curr_partition_index == (numBuff - 2) && eosl == true){
					//have finish all the partition, finish the join
					//partition index 0 - numBuff - 2
					return null;
				}else{
					if(curr_partition_index == -1 || eosl == true){
						//start a new partition if at the end of the current partition of left table
						curr_partition_index++;
					    in_lfname = "HJtempLeft-" + String.valueOf(curr_partition_index) + this.hashCode(); 
					    in_rfname = "HJtempRight-" + String.valueOf(curr_partition_index)+ this.hashCode(); 
					}
					//if not at end of current partition for left table, still remain in current partition
					//start a new round for right table for the rest of the left table pages 
				    try{
					    in_left = new ObjectInputStream(new FileInputStream(in_lfname));
					    in_right = new ObjectInputStream(new FileInputStream(in_rfname));
				    }catch(IOException io){
				    	System.out.println("HashJoin:error in opening outstream for join phase" + "in_lfname " + in_lfname + " in_rfname " + in_rfname);
				    	continue;
				    }
				    //initialize in-memory hashtable to store new partitions of each partition of left table
				    //index from 0 - numBuff-3
				    in_memory_ht = new Batch[numBuff - 2];
				    for(int i = 0; i < numBuff - 2; i ++){
				    	in_memory_ht[i] = new Batch(left_batchsize);
				    }
				    lcurs = 0;
				    rcurs = 0;
				    eosl = false;
				    eosr = false;
				    inputbatch_right = null;
				    try{
				    	//hash the current partition of left table using different hash function
				    	try{
				    		inputbatch = (Batch)in_left.readObject();
				    		while(inputbatch == null) inputbatch = (Batch)in_left.readObject();
				    	}catch(IOException io){
				    		eosl = true;
				    		eosr = true;
				    			
				    		continue;
				    	}
				        int key;
				        while(true){
					        //check cursor
					        if(lcurs >= inputbatch.size()){
					        	try{
					        		//load a new left table page for in-memory partition
					        		inputbatch = (Batch)in_left.readObject();
					        		while(inputbatch == null || inputbatch.isEmpty()) inputbatch = (Batch)in_left.readObject();
					        		lcurs = 0;
					        	}catch(IOException io){
					        		//no more page for this partition, finish this partition
					        		eosl = true;
					        		System.out.println("reaching here: run out of left page");
					        		break;
					        	}	
					        }
					        key = (inputbatch.elementAt(lcurs).dataAt(leftindex).hashCode())%(numBuff-2);

					        //check whether the bucket is full; if yes, stop reading, matching right table tuples first,store rest of non-partitioned left table tuples in original file
					        if(in_memory_ht[key].size() >= left_batchsize){
					        	eosl = false;
					        	String tempFile = "tempFile";
					        	ObjectOutputStream temp = new ObjectOutputStream(new FileOutputStream(tempFile));
					        	//write the rest tuples in current input buffer back
					        	if(lcurs <= inputbatch.size() - 1){
					        		for(int i = 0; i <= lcurs ; i++){
					        			inputbatch.remove(0);
					        		}
					        		temp.writeObject(inputbatch);
					        	}
					        	lcurs = 0;
					        	try{
					        		while(true){
					        			temp.writeObject(in_left.readObject());
					        		}
					        	}catch(IOException e){
					        		//reach end of left table partition
					        		try{
					        			temp.close();
					        		}catch(IOException io){
					        			System.out.println("HashJoin:Error in temporary file reading");
					        		}			
					        	}
					        	ObjectOutputStream out_left = new ObjectOutputStream(new FileOutputStream(in_lfname));
					        	ObjectInputStream temp_in = in_right = new ObjectInputStream(new FileInputStream(tempFile));
					        	try{
					        		while(true){
					        			out_left.writeObject(temp_in.readObject());
					        		}
					        	}catch(IOException io){
					        		//write finish
					        		try{
					        			out_left.close();
					        		}catch(IOException io1){
					        			System.out.println("Hashjoin: error in closing temp write fil " + in_lfname);
					        		}
					        	}
					        	System.out.println("reaching here: before break");
					        	break;
					        }
					        //the bucket is not full
					        System.out.println("reaching here: constructing in memory ht");
					        in_memory_ht[key].add(inputbatch.elementAt(lcurs++));
					        	

				        }
				        lcurs = 0;
				        rcurs = 0;
				        System.out.println("reaching here: finish constructing in memory ht");

				        //load the right table page for matching
				        try{
				        		
				        	inputbatch_right = (Batch)in_right.readObject();//handle the case if the partition is empty
				        	while(inputbatch_right == null || inputbatch_right.size() == 0) inputbatch_right = (Batch)in_right.readObject();
				        	rcurs = 0;
				        }catch(IOException io){
				        	eosr = true;
				        	try{
				        		in_right.close();
				        	}catch(IOException io1){
				        		System.out.println("HashJoin:error in closingfile");
				        	}
				        	continue;
				        }
				        	
				    }catch(IOException io){
				        System.out.println("HashJoin:end of input file");
				        lcurs = 0;
				        eosl = true;
				        continue;
				    } catch (ClassNotFoundException e) {
						System.out.println("HashJoin:Some error in deserialization ");
						 System.exit(1);
					}  		    
				}
			}
		if(rcurs == inputbatch_right.size()){
			//load a new right table page
			try{
				inputbatch_right = (Batch)in_right.readObject();
				while(inputbatch_right == null || inputbatch_right.isEmpty()) inputbatch_right = (Batch)in_right.readObject();
			}catch(IOException io){
				//reach end of right table partition
				eosr = true;
				try{
					in_right.close();
				}catch(IOException io1){
					System.out.println("HashJoin:error in closing file");
				}
				continue;
			} catch (ClassNotFoundException e) {
			    System.out.println("HashJoin:Some error in deserialization ");
			    System.exit(1);
			}	
		}
		//generate result tuple
	    Tuple lefttuple;
	    Tuple righttuple;// = inputbatch_right.elementAt(rcurs);
	    int key;// = righttuple.dataAt(rightindex).hashCode()%(numBuff-2);
	    Batch curr_bucket;// = in_memory_ht[key];
	    while(rcurs < inputbatch_right.size()){
	    	righttuple = inputbatch_right.elementAt(rcurs);
	    	key = righttuple.dataAt(rightindex).hashCode()%(numBuff-2);
	    	curr_bucket = in_memory_ht[key];
	    	while(lcurs < curr_bucket.size()){
	    		//match each left table tuple in the partition
	    		lefttuple = curr_bucket.elementAt(lcurs);
			    if(lefttuple.checkJoin(righttuple,leftindex,rightindex)){
					Tuple outtuple = lefttuple.joinWith(righttuple);
					System.out.println("matching: " + outtuple.data());
					outbatch.add(outtuple);
					if(outbatch.isFull()){
					    return outbatch;
					}
				}
			    lcurs++;
	    	}
	    	//finish with current right table tuple
	    	if(lcurs >= curr_bucket.size()){
	    		rcurs++;
	    		lcurs = 0;
	    	}
	    	if(rcurs >= inputbatch_right.size()) continue;
	    }
	    /*
	    while(lcurs < curr_bucket.size()){
	    	lefttuple = curr_bucket.elementAt(lcurs++);
	    	if(lcurs >= curr_bucket.size()){
	    		rcurs++;
	    		lcurs = 0;
	    	}
		    if(lefttuple.checkJoin(righttuple,leftindex,rightindex)){
				Tuple outtuple = lefttuple.joinWith(righttuple);
				outbatch.add(outtuple);
				if(outbatch.isFull()){
				    return outbatch;
				}
			}
	    }
	    */
	}
	
	return outbatch;
    }



    /** Close the operator */
    public boolean close(){

    	for(int i = 0; i < numBuff -1;i++){
    		String right_fname = "HJtempRight-" + String.valueOf(i) + this.hashCode();
    		String left_fname = "HJtempLeft-" + String.valueOf(i) + this.hashCode();
    		File f = new File(right_fname);
    		f.delete();
    		f = new File(left_fname);
    		f.delete();
    	}

	return true;

    }


}
