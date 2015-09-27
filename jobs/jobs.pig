--cd jobs;
register StemTokens.jar;
register ComputeLevenshtein.jar;

jobs1 = load '20140212_descriptions.csv' using PigStorage( ',' ) as ( jobid: chararray, jobdesc: chararray );
jobs2 = load '20140213_descriptions.csv' using PigStorage( ',' ) as ( jobid: chararray, jobdesc: chararray );
jobs = union jobs1, jobs2;

stopwords = load 'stopwords-en.txt' as ( stopword: chararray );
dictionary = load 'dictionary.txt' as ( dictword: chararray );

rawtokens = foreach jobs generate jobid, FLATTEN( TOKENIZE( LOWER( REPLACE( jobdesc, '([^a-zA-Z0-9\\s\']+)', ' ' ) ) ) ) as jobword;
groupedtokens = group rawtokens by jobword, stopwords by stopword;
filteredtokens = foreach ( filter groupedtokens by IsEmpty( stopwords ) ) generate flatten( rawtokens );

stemmedtokens = foreach filteredtokens generate jobid, StemTokens( jobword ) as jobword;
mappedtokens = group stemmedtokens by jobword, dictionary by dictword;
matchedtokens = foreach ( filter mappedtokens by not IsEmpty( dictionary ) ) generate flatten( stemmedtokens );
unmatchedtokens = foreach( filter mappedtokens by IsEmpty( dictionary ) ) generate flatten( stemmedtokens );

unmatchedtokensunique = distinct ( foreach unmatchedtokens generate jobword );

crossedtokens = cross unmatchedtokensunique, dictionary;
crossedtokensdist = foreach crossedtokens generate jobword, dictword, ComputeLevenshtein( jobword, dictword ) as dist;
groupedtokens = group crossedtokensdist by jobword;

tokensmatch = foreach groupedtokens {
		mintoken = order crossedtokensdist by dist asc;
		mintoken = limit mintoken 1;
		generate mintoken;
	};

imputedtokens = foreach tokensmatch generate FLATTEN( mintoken.jobword ) as jobword, FLATTEN( mintoken.dictword ) as matchedword;
joinedtokens = join unmatchedtokens by jobword, imputedtokens by jobword using 'replicated';
correctedtokens = foreach joinedtokens generate jobid, matchedword as jobword;

combinedtokens = union matchedtokens, correctedtokens;
groupedcombinedtokens = group combinedtokens by jobid;
jobtokens = foreach groupedcombinedtokens generate group as jobid, combinedtokens.jobword as jobword;

fs -rm -r jobtokens;
store jobtokens into 'jobtokens' using PigStorage( '\t' );
fs -getmerge jobtokens jobtokens;

