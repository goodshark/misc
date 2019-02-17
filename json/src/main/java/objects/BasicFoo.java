package objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Created by dengrenbo on 19/2/16.
 */
public class BasicFoo {

    private int id;
    private String desc;
    private Map<String, String> info;

    /**
     * 序列化
     * 以get开头的method, 被jackson序列化时, 都会自动成为json字符串中的一个key(get后的内容)
     * 此key对应的值即是method返回的值
     *
     * 反序列化
     * 将json字符串中的key反序列化为类中的field
     */

    public Map<String, String> getInfo() {
        return info;
    }

    public void setInfo(Map<String, String> info) {
        this.info = info;
    }

    public String getDesc() {
        return desc;
    }

    public void hahaDesc(String desc) {
        this.desc = desc;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }


}
