import java.io.IOException;

public class QrySopAnd extends QrySop {

	@Override
	public double getScore(RetrievalModel r) throws IOException {

		if(!this.docIteratorHasMatchAll(r)){
			return 0.0;
		}
		if (r instanceof RetrievalModelUnrankedBoolean) {
			return this.getScoreUnrankedBoolean (r);
		} else if( r instanceof RetrievalModelRankedBoolean){
			return this.getScoreRankedBoolean(r);
		}
		else{
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the AND operator.");
		}
	}

	private double getScoreUnrankedBoolean(RetrievalModel r) {
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
			//Return the min value of the matches
			double min = Double.MAX_VALUE;
			for(Qry arg : this.args){
				if(!arg.docIteratorHasMatch(r)) continue;
				
				if(this.docIteratorGetMatch() == arg.docIteratorGetMatch()){
					if(arg instanceof QrySop){
						QrySop sopArg = (QrySop) arg;
						double score = sopArg.getScore(r);
						if(min > score){
							min = score;
						}
					}
				}
			}
			if(min == Double.MAX_VALUE) min = 0.0;
			return min;
		}
	}

	@Override
	public boolean docIteratorHasMatch(RetrievalModel r) {
		return this.docIteratorHasMatchAll (r);
	}

}
