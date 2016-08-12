/**
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;
import java.util.*;

/**
 *  The NEAR operator for all retrieval models.
 */
public class QryIopNear extends QryIop {

	int distance = 0;	
	public QryIopNear(int distance) {
		this.distance = distance;
	}

	/**
	 *  Evaluate the query operator; the result is an internal inverted
	 *  list that may be accessed via the internal iterators.
	 *  @throws IOException Error accessing the Lucene index.
	 */
	protected void evaluate () throws IOException {

		//  Create an empty inverted list.  If there are no query arguments,
		//  that's the final result.

		this.invertedList = new InvList (this.getField());

		if (args.size () == 0) {
			return;
		}

		//Finds the smallest document list
		int min = Integer.MAX_VALUE;
		QryIop q_0 = null;
		for (Qry q_i: this.args) {
			int freq = ((QryIop) q_i).getDf();
			if(freq < min){
				q_0 =(QryIop) q_i;
				min = freq;
			}
		}
		
		while (q_0.docIteratorHasMatch(null)) {

			int current_doc = q_0.docIteratorGetMatch();
			//move all document pointers to q0 pointer. If document does not exist, skip it
			for (Qry q_i: this.args) {
				if(q_i == q_0) continue;
				q_i.docIteratorAdvanceTo(current_doc);
				
				if(!q_i.docIteratorHasMatch(null) || q_i.docIteratorGetMatch() != current_doc){
					q_0.docIteratorAdvancePast(current_doc);
					break;
				}

			}
			if(!q_0.docIteratorHasMatch(null) || q_0.docIteratorGetMatch() != current_doc) continue;


			//Here all pointers are in the same documents.

			List<Integer> positions = new ArrayList<Integer>();

			int last = -1;
			QryIop firstArg = this.getArg(0);
			while(firstArg.locIteratorHasMatch()){
				for (Qry q_i: this.args) {

					QryIop q_iop = ((QryIop) q_i);
					if(!q_iop.locIteratorHasMatch()) {
						last = -1;
						break;
					}
					

					if(last == -1){
						last = q_iop.locIteratorGetMatch();
						q_iop.locIteratorAdvance();
						continue;
					}
					else{
						q_iop.locIteratorAdvancePast(last);
						
						if(q_iop.locIteratorHasMatch () && 
								q_iop.locIteratorGetMatch() - last <= distance){
							last = q_iop.locIteratorGetMatch();
						}else{
							
							last = -1;
							break;
						}
					}
				}
				if(last != -1){
					positions.add(last);
				}
			}
			q_0.docIteratorAdvancePast(current_doc);
			if(positions.size() == 0) continue;
			Collections.sort (positions);
			this.invertedList.appendPosting (current_doc, positions);
		}
	}

}
