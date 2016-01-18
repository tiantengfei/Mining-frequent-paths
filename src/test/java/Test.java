import com.ttfworld.CandidateHelper;

/**
 * Created by ttf on 16-1-18.
 */
public class Test {

    public static void main(String[] args) {
        String p1 = "201~202";
        String p2 = "202~203";

        CandidateHelper candidateHelper = CandidateHelper.getInsance();

        System.out.println(candidateHelper.getHead(p1));
        System.out.println(candidateHelper.getTail(p1));

        if(candidateHelper.getTail(p1).equals(candidateHelper.getHead(p2)))
            System.out.println(candidateHelper.getNewCandinate(p1, p2));
    }
}
