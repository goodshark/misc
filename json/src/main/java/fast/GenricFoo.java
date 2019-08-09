package fast;

import java.util.ArrayList;
import java.util.List;

public class GenricFoo<T> {
    private int num;
    private List<T> list = new ArrayList<>();

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }
}
