package fast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FastFoo {
    private int no;
    private String name;
    private List<String> goods = new ArrayList<>();
    private Map<String, List<String>> props = new HashMap<>();

    public int getNo() {
        return no;
    }

    public void setNo(int no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getGoods() {
        return goods;
    }

    public void setGoods(List<String> goods) {
        this.goods = goods;
    }

    public Map<String, List<String>> getProps() {
        return props;
    }

    public void setProps(Map<String, List<String>> props) {
        this.props = props;
    }
}
