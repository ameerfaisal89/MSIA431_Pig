import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class ComputeLevenshtein extends EvalFunc <Integer> {
	public Integer exec( Tuple input ) throws IOException {
		if ( input == null || input.size( ) == 0 )
			return null;

		try {
			String str1 = (String) input.get( 0 );
			String str2 = (String) input.get( 1 );

			if ( str1.length( ) > 0 && str2.length( ) > 0 && str1.charAt( 0 ) != str2.charAt( 0 ) )
				return Integer.MAX_VALUE;
			return Levenshtein.getLevenshteinDistance( str1, str2 );
		}
		catch ( Exception e ) {
			throw new IOException( "Caught exception processing input row ", e );
		}
	}
}

