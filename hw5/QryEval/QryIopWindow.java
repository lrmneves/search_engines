import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
public class QryIopWindow extends QryIop {
	int range;
	public QryIopWindow(int r) {
		this.range = r;
	}

	@Override
	protected void evaluate() throws IOException {
		//  Create an empty inverted list.  If there are no query arguments,
		//  that's the final result.

		this.invertedList = new InvList (this.getField());

		if (args.size () == 0) {
			return;
		}

		//Finds the smallest document list to make comparison smaller
		int min = Integer.MAX_VALUE;
		QryIop q_0 = null;
		for (Qry q_i: this.args) {
			int freq = ((QryIop) q_i).getDf();
			if(freq < min){
				q_0 =(QryIop) q_i;
				min = freq;
			}
		}

		//Iterate through smallest list
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
			ArrayList<QryObject> qryObjectList = new ArrayList<>(this.args.size());
			

			for(int i = 0; i < this.args.size();i++){
				QryIop q_iop = ((QryIop) this.args.get(i));
				if(!q_iop.locIteratorHasMatch()) {
					//if there is no more of the term in the doc, stop here
					break;
				}
				qryObjectList.add(new QryObject(q_iop.locIteratorGetMatch(),q_iop));
			}
			if (qryObjectList.size() < this.args.size()) continue;
			
			Collections.sort(qryObjectList);
			
			QryObject currentObject = qryObjectList.get(0);
			boolean done = false;
			while(!done){
				int minLocation = currentObject.qry.locIteratorGetMatch();
				int maxLocation = qryObjectList.get(qryObjectList.size()-1).qry.locIteratorGetMatch();
				if(maxLocation - minLocation < this.range){
					positions.add(minLocation);
					for(QryObject obj : qryObjectList){
						obj.qry.locIteratorAdvance();
						if(!obj.qry.locIteratorHasMatch()){
							done = true;
							break;
						}
						obj.updateFirstIndex();
					}
				}else{
					currentObject.qry.locIteratorAdvance();
					if(!currentObject.qry.locIteratorHasMatch()){
						done = true;
						break;
					}
					currentObject.updateFirstIndex();
				}
				Collections.sort(qryObjectList);
				currentObject = qryObjectList.get(0);
			}
			q_0.docIteratorAdvancePast(current_doc);
			if(positions.size() == 0) continue;
			Collections.sort (positions);
			this.invertedList.appendPosting (current_doc, positions);
		}
	}
}




class QryObject implements Comparable<QryObject>{
	int firstIndex;
	QryIop qry;

	QryObject(int i,QryIop q){
		this.firstIndex = i;
		this.qry = q;
	}

	@Override
	public int compareTo(QryObject o) {
		return this.firstIndex - o.firstIndex;
	}
	public void updateFirstIndex(){
		this.firstIndex = qry.locIteratorGetMatch();
	}
}
