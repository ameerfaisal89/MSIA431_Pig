cd tweets;

tweets1 = load 'tweets_20121102.txt' using PigStorage( '|' ) as
( timestamp: chararray,
  userid: long,
  userhandle: chararray,
  username: chararray,
  userinfo: chararray,
  language: chararray,
  location: chararray,
  latitute: double,
  longitude: double,
  text: chararray );

tweets2 = load 'tweets_20121103.txt' using PigStorage( '|' ) as
( timestamp: chararray,
  userid: long,
  userhandle: chararray,
  username: chararray,
  userinfo: chararray,
  language: chararray,
  location: chararray,
  latitute: double,
  longitude: double,
  text: chararray );

tweets = union tweets1, tweets2;
  
poswords = load 'good.txt' as ( word: chararray );
negwords = load 'bad.txt' as ( word: chararray );

tweets = rank tweets;
prunedtweets = foreach tweets generate rank_tweets as tweetid, REPLACE( text, '([^a-zA-Z0-9\\s\']+)', ' ' ) as text;
tokens = foreach prunedtweets generate tweetid, FLATTEN( TOKENIZE( text ) ) as tweetword;
tokensprocessed = foreach tokens generate tweetid, LOWER( tweetword ) as tweetword;
--tokensprocessed = foreach tokens generate tweetid, FLATTEN( REGEX_EXTRACT_ALL( LOWER( tweetword ), '[^a-z0-9]*([a-z0-9]*)[^a-z0-9]*' ) ) as tweetword;

poswordsmap = join tokensprocessed by tweetword left outer, poswords by word using 'replicated';
negwordsmap = join tokensprocessed by tweetword left outer, negwords by word using 'replicated';

groupedposwords = group poswordsmap by tweetid;
groupednegwords = group negwordsmap by tweetid;

poscounts = foreach groupedposwords generate group, COUNT( poswordsmap.word ) as poscount;
negcounts = foreach groupednegwords generate group, COUNT( negwordsmap.word ) as negcount;

joinedcounts = join poscounts by group, negcounts by group;
sentiment = foreach joinedcounts generate poscounts::group as tweetid, ( poscount - negcount ) as score;

fs -rm -r sentiment;
store sentiment into 'sentiment';

sentimentflag = foreach sentiment generate tweetid, ( score > 0? 1:0 ) as pos, ( score < 0? 1:0 ) as neg;
sentimentgroup = group sentimentflag all;
sentimentcounts = foreach sentimentgroup generate SUM( sentimentflag.pos ) as postweets, SUM( sentimentflag.neg ) as negtweets;

fs -rm -r sentimentcounts;
store sentimentcounts into 'sentimentcounts';
fs -getmerge sentimentcounts sentimentcounts;

