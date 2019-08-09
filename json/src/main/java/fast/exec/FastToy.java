package fast.exec;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import fast.FastFoo;
import fast.GenricFoo;

import java.util.*;

public class FastToy {

    public static void main(String[] args) {

        useNodes();
//        fastGenric();
    }

    private static void basicUse() {
        // serialize
        FastFoo foo = new FastFoo();
        foo.setName("lisa");
        foo.setNo(200);
        List<String> goods = new ArrayList<>();
        goods.add("item-1");
        goods.add("item-2");
        foo.setGoods(goods);
        Map<String, List<String>> map = new HashMap<>();
        map.put("attrs", new ArrayList<String>(){{add("very");add("luck");}});
        foo.setProps(map);
        String str = JSON.toJSONString(foo);
        System.out.println("str: " + str);
        // deserialize
        FastFoo resFoo = JSON.parseObject(str, FastFoo.class);
        System.out.println(resFoo);
    }

    private static void fastGenric() {
        // serialize
        GenricFoo<FastFoo> genricFoo = new GenricFoo<>();
        genricFoo.setNum(123);

        FastFoo foo = new FastFoo();
        foo.setName("jack");
        foo.setNo(123);
        List<String> goods = new ArrayList<>();
        goods.add("good1");
        goods.add("good2");
        foo.setGoods(goods);
        Map<String, List<String>> map = new HashMap<>();
        map.put("m1", new ArrayList<String>(){{add("very");add("luck");}});
        foo.setProps(map);

        List<FastFoo> list = new ArrayList<>();
        list.add(foo);

        genricFoo.setList(list);
        String str = JSON.toJSONString(genricFoo);
        System.out.println("str: " + str);

        // deserialize
        // will lose the type info
//        GenricFoo<FastFoo> res = JSON.parseObject(str, GenricFoo<>.class);
        // use TypeReference will deserialize real type
        GenricFoo<FastFoo> res = JSON.parseObject(str, new TypeReference<GenricFoo<FastFoo>>(){});
        System.out.println(res);
    }

    private static void useNodes() {
        // serialize
        FastFoo foo = new FastFoo();
        foo.setName("harry");
        foo.setNo(100);
        List<String> goods = new ArrayList<>();
        goods.add("letter");
        goods.add("branch");
        foo.setGoods(goods);
        Map<String, List<String>> map = new HashMap<>();
        map.put("location", new ArrayList<String>(){{add("very");add("luck");}});
        foo.setProps(map);
        String str = JSON.toJSONString(foo);
        System.out.println("str: " + str);
        // deserialize to the node
        JSONObject jsonObject = JSON.parseObject(str);
        String item2 = jsonObject.getJSONArray("goods").getString(1);
        String value = jsonObject.getJSONObject("props").getJSONArray("location").getString(1);
        System.out.println("item2: " + item2 + ", value: " + value);
    }
}
