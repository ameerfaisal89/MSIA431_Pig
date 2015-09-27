import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class StemTokens extends EvalFunc <String> {
	public String exec( Tuple input ) throws IOException {
		Porter porter = new Porter( );

		if ( input == null || input.size( ) == 0 )
			return null;

		try {
			String str = (String) input.get( 0 );
			return porter.stripAffixes( str );
		}
		catch ( Exception e ) {
			throw new IOException( "Caught exception processing input row ", e );
		}
	}
}

