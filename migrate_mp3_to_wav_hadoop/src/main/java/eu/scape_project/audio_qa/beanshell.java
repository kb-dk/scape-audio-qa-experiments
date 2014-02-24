package eu.scape_project.audio_qa;

import java.util.AbstractList;
import java.util.List;

/**
 * Created by bolette on 2/14/14.
 */
public class beanshell {

    public static void sfjlksd(AbstractList<String> lines){
        List<String> formatted = null;

        int returnCode = 0;
        for (int i = 0; i < lines.size(); i++){
            String line=lines.get(i);
            if (line.matches("^\\d+\\s+")){
                returnCode = Integer.parseInt(line.substring(0,3));
                line = line.replaceFirst("^\\d+\\s+","");
            }
            formatted.add(line+","+returnCode);
        }

    }
}
