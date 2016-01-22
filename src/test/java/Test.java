import com.ttfworld.CandidateHelper;
import com.ttfworld.Path;

/**
 * Created by ttf on 16-1-18.
 */
public class Test {

    public static void main(String[] args) {

        String p1 = "201~202~204~208~203~202~205\t110";
        String p2 = "202~203~205\t123_c";

        CandidateHelper candidateHelper = CandidateHelper.getInsance();

       // System.out.println(candidateHelper.getHead(p1));
       // System.out.println(candidateHelper.getTail(p1));


       if(candidateHelper.getTail(p1).equals(candidateHelper.getHead(p2)))
            System.out.println(candidateHelper.getNewCandinate(p1, p2));



        Path path = new Path(p1);
        Path can = new Path(p2);
/**
        System.out.println(path.getWebsites());
        System.out.println(can.getWebsites());
        System.out.println(path.getIsCandidate());
        System.out.println(can.getIsCandidate());
 **/
        System.out.println(path.getPath());
        System.out.println(can.getPath());
        System.out.println(candidateHelper.isContain(path, 0 , can , 0));

        String ss = "2015-11-02 02:14:25~20012~2240~5217~2303~2180   23";
        System.out.println(ss.split("\t").length);
        System.out.println(ss.split("\t")[0]);


    }
}
