package objects;

import java.util.List;

/**
 * Created by dengrenbo on 19/2/17.
 */

public class ComplexFoo {
    private int id;
    private String name;
    private BasicFoo foo;
    private List<String> strs;

    public List<String> getStrs() {
        return strs;
    }

    public void setStrs(List<String> strs) {
        this.strs = strs;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public BasicFoo getFoo() {
        return foo;
    }

    public void setFoo(BasicFoo foo) {
        this.foo = foo;
    }

}
