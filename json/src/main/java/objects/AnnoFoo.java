package objects;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dengrenbo on 19/2/16.
 */

// 序列化, JsonPropertyOrder将保证info和abc字段在json字符串的最前面
@JsonPropertyOrder({"info", "abc"})
public class AnnoFoo {

    private int id;
    private Map<String, String> data = new HashMap<>();
    private String good;
    private String abc;
    private String info;

    public String getGood() {
        return good;
    }

    public void setGood(String good) {
        this.good = good;
    }

    public String getAbc() {
        return abc;
    }

    public void setAbc(String abc) {
        this.abc = abc;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    // 序列化的时候, 将此map中的所有kv在json中展开
    @JsonAnyGetter
    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    // 序列化,JsonProperty, JsonGetter 会将不是以get开头的method, 变得和以get开头的method在序列化时行为一样
    @JsonProperty
    public String testValue() {
        return "this is a test";
    }

    // 将testValue2在json字符串的字段中重新命名为haha
    @JsonGetter("haha")
    public String testValue2() {
        return "this is a test 2";
    }

    // 反序列化, json字符串中出现类中对应不上的字段, 将此字段对应的kv放到此map中
    @JsonAnySetter
    public void setKV(String k, String v) {
        data.put(k, v);
    }
}
