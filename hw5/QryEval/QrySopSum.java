/**
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;

/**
 *  The OR operator for all retrieval models.
 */
public class QrySopSum extends QrySop {

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
		if (r instanceof RetrievalModelBM25){
			return getScoreBM25(r);
		}else {
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the OR operator.");
		}
	}

	private double getDefaultScoreBM25(RetrievalModel r){
		return 0.0;
	}

	/**
	 *  getScore for the BM25 retrieval model.
	 *  @param r The retrieval model that determines how scores are calculated.
	 *  @return The document score.
	 *  @throws IOException Error accessing the Lucene index
	 */

	private double getScoreBM25(RetrievalModel r) throws IOException {

		double sum = 0.0;
		for(Qry arg : this.args){
			if(arg instanceof QrySop){
				if(!arg.docIteratorHasMatch(r)) continue;

				if(this.docIteratorGetMatch() == arg.docIteratorGetMatch()){
					QrySop sopArg = (QrySop) arg;

					sum+=sopArg.getScore(r);
				}
			}
		}
		return sum;

	}

	@Override
	public double getDefaultScore(RetrievalModel r) throws IOException {
		if (r instanceof RetrievalModelBM25){
			return getDefaultScoreBM25(r);
		}else {
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the OR operator.");
		}
	}

}
