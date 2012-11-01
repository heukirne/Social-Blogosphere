import nl.peterbloem.powerlaws.*;
import nl.peterbloem.util.*;

import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;

public class PowerLaw {

    public static void main(String[] args) throws Exception {

    	List<Double> data = Arrays.asList(1d,2d,10d, 12d, 10d);
    	Continuous model = Continuous.fit(data).fit();
    	double exponent = Continuous.fit(data).fit().exponent();
    	double significance = model.significance(data, 100);
        System.out.println( "exponent:"+exponent );
        System.out.println( "significance:"+significance );

		Continuous distribution = new Continuous(340, 2.5);
        List<Double> generated = distribution.generate(1000); 
        model = Continuous.fit(generated).fit();
    	exponent = Continuous.fit(generated).fit().exponent();
    	significance = model.significance(generated, 100);
        System.out.println( "exponent:"+exponent );
        System.out.println( "significance:"+significance );

    }	
	
}
