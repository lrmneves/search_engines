/**
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;

/**
 *  The OR operator for all retrieval models.
 */
public class QrySopOr extends QrySop {

	/**
	 *  Indicates whether the query has a match.
	 *  @param r The retrieval model that determines what is a match
	 *  @return True if the query matches, otherwise false.
	 */
	public boolean docIteratorHasMatch (RetrievalModel r) {
		return this.docIteratorHasMatchMin (r);
	}

	/**
	 *  Get a score for the document that docIteratorHasMatch matched.
	 *  @param r The retrieval model that determines how scores are calculated.
	 *  @return The document score.
	 *  @throws IOException Error accessing the Lucene index
	 */
	public double getScore (RetrievalModel r) throws IOException {
		if(!this.docIteratorHasMatch(r)){
			return getDefaultScore(r);
		}
		if (r instanceof RetrievalModelUnrankedBoolean) {
			return this.getScoreUnrankedBoolean (r);
		}else if(r instanceof RetrievalModelRankedBoolean) {
			return this.getScoreRankedBoolean(r);
		}else if (r instanceof RetrievalModelIndri){
			return this.getScoreIndri(r);
		}
		else {
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the OR operator.");
		}
	}

	private double getScoreIndri(RetrievalModel r) throws IOException {
		
		double prod = 1.0;
		
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
				prod*= (1 - score);
			}
		}

		return 1 - prod;
	}

	/**
	 *  getScore for the UnrankedBoolean retrieval model.
	 *  @param r The retrieval model that determines how scores are calculated.
	 *  @return The document score.
	 *  @throws IOException Error accessing the Lucene index
	 */
	private double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {
		if (! this.docIteratorHasMatchCache()) {
			return 0.0;
		} else {
			return 1.0;
		}
	}
	private double getScoreRankedBoolean(RetrievalModel r) throws IOException {
		if (! this.docIteratorHasMatchCache()) {
			return 0.0;
		} else {
			//Return the max value of the matches
			double max = Double.MIN_VALUE;
			for(Qry arg : this.args){
				if(arg instanceof QrySop){
					if(!arg.docIteratorHasMatch(r)) continue;

					if(this.docIteratorGetMatch() == arg.docIteratorGetMatch()){
						QrySop sopArg = (QrySop) arg;
						double score = sopArg.getScore(r);
						if(max < score){
							max = score;
						}
					}
				}
			}
			if(max == Double.MIN_VALUE) max = 0.0;
			return max;
		}


	}

	@Override
	public double getDefaultScore(RetrievalModel r) throws IOException {
		if (r instanceof RetrievalModelUnrankedBoolean) {
			return this.getDefaultScoreUnrankedBoolean (r);
		}else if(r instanceof RetrievalModelRankedBoolean) {
			return this.getDefaultScoreRankedBoolean(r);
		}else if (r instanceof RetrievalModelIndri){
			return this.getDefaultScoreIndri(r);
		}
		else {
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the OR operator.");
		}	}

	private double getDefaultScoreIndri(RetrievalModel r) throws IOException {
		return this.getScoreIndri(r);
	}

	private double getDefaultScoreRankedBoolean(RetrievalModel r) {
		return 0.0;
	}

	private double getDefaultScoreUnrankedBoolean(RetrievalModel r) {
		return 0.0;
	}

}
