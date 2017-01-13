package Top20Restaurants;

import java.util.Comparator;

/**
 * Created by dongyueli on 12/10/16.
 */
class MyStarsComp1 implements Comparator<Stars> {
    public int compare(Stars s1, Stars s2) {
        if(s1.getStar() > s2.getStar()) {
            return 1;
        } else {
            return -1;
        }
    }
}
