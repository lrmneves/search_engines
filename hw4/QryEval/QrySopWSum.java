/**
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;

/**
 *  The OR operator for all retrieval models.
 */
public class QrySopWSum extends QrySop {
	Double norm = null;
	/**
	 *  Indicates whether the query has a match.
	 *  @param r The retrieval model that determines what is a match
	 *  @return True if the query matches, otherwise false.
	 */
	public boolean docIteratorHasMatch (RetrievalModel r) {
		return this.docIteratorHasMatchMin (r);
	}
	public void initNorm(){
		if (norm == null){
			norm = 0.0;
			for (double w : this.weights){
				norm+=w;
			}
		}
	}

	/**
	 *  Get a score for the document that docIteratorHasMatch matched.
	 *  @param r The retrieval model that determines how scores are calculated.
	 *  @return The document score.
	 *  @throws IOException Error accessing the Lucene index
	 */
	public double getScore (RetrievalModel r) throws IOException {
		initNorm();
		if(!this.docIteratorHasMatch(r)){
			return getDefaultScore(r);
		}
		if(r instanceof RetrievalModelIndri){
			return getScoreIndri(r);
		}else {
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the OR operator.");
		}
	}

	
	private double getDefaultScoreIndri(RetrievalModel r) throws IOException {
		initNorm();
		return getScoreIndri(r);	
	}

	private double getScoreIndri(RetrievalModel r) throws IOException {
		
		double sum = 0.0;
		int index = 0;
		for(Qry arg : this.args){
			
			if(arg instanceof QrySop){
				QrySop sopArg = (QrySop) arg;
				double score = 0.0;
				if(arg.docIteratorHasMatch(r) &&
						arg.docIteratorGetMatch() == ((RetrievalModelIndri) r).currentDoc ){
					score = sopArg.getScore(r);
				}else{
					score = sopArg.getDefaultScore(r);
				}
				sum+= score*this.weights.get(index)/norm;
			}
			index+=1;
		}

		return sum;
	}

	

	@Override
	public double getDefaultScore(RetrievalModel r) throws IOException {
		if( r instanceof RetrievalModelIndri){
			return getDefaultScoreIndri(r);
		}else{
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the OR operator.");
		}
	}

}
