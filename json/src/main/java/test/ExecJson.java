package test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import objects.AnnoFoo;
import objects.BasicFoo;
import objects.ComplexFoo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by dengrenbo on 19/2/16.
 */

public class ExecJson {
    private void basicUse() throws Exception {
        // serialize
        BasicFoo fromBasicFoo = new BasicFoo();
        fromBasicFoo.setId(123);
        fromBasicFoo.hahaDesc("test only");
        Map<String, String> info = new HashMap<>();
        info.put("hehe", "100");
        info.put("good", "123");
        fromBasicFoo.setInfo(info);
        ObjectMapper om = new ObjectMapper();
        String res = om.writeValueAsString(fromBasicFoo);
        System.out.println("res: " + res);
        // deserialize
        BasicFoo toBasicFoo = om.readValue(res, BasicFoo.class);
        System.out.println("id: " + toBasicFoo.getId());
        System.out.println("desc: " + toBasicFoo.getDesc());
    }

    private void collectionUse() throws Exception {
        // serialize
        List<BasicFoo> list = new LinkedList<>();
        int numId = 200;
        for (int i = 0; i < 2; i++) {
            BasicFoo fromBasicFoo = new BasicFoo();
            fromBasicFoo.setId(numId++);
            fromBasicFoo.hahaDesc("just present");
            Map<String, String> info = new HashMap<>();
            info.put("where", "china");
            info.put("zone", "beijing");
            fromBasicFoo.setInfo(info);
            list.add(fromBasicFoo);
        }
        ObjectMapper om = new ObjectMapper();
        String res = om.writeValueAsString(list);
        System.out.println("res: " + res);
        // deserialize
        List<BasicFoo> toList = om.readValue(res, new TypeReference<List<BasicFoo>>() {});
        for (int i = 0; i < toList.size(); i++) {
            System.out.println("id: " + toList.get(i).getId());
        }
    }

    private void nestedUse() throws Exception {
        ComplexFoo foo = new ComplexFoo();
        foo.setId(1);
        foo.setName("test");
        BasicFoo basicFoo = new BasicFoo();
        basicFoo.setId(123);
        basicFoo.hahaDesc("just present");
        Map<String, String> info = new HashMap<>();
        info.put("ufo", "1-abc");
        info.put("speed", "2m");
        basicFoo.setInfo(info);
        foo.setFoo(basicFoo);
        List<String> list = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            list.add("num-"+i);
        }
        foo.setStrs(list);
        ObjectMapper om = new ObjectMapper();
        String complexRes = om.writeValueAsString(foo);
        System.out.println("complex res: " + complexRes);

        // deserialize
        ComplexFoo toComplexFoo = om.readValue(complexRes, ComplexFoo.class);
        System.out.println("toComplexFoo id: " + toComplexFoo.getId());

        // deserialize to JsonNode, only need inner single value, not need all value
        JsonNode node = om.readTree(complexRes);
        String innerStr = node.get("foo").get("desc").toString();
        System.out.println("inner string: " + innerStr);

    }

    private void annotationUse() throws Exception {
        // serialize
        AnnoFoo annoFoo = new AnnoFoo();
        annoFoo.setId(100);
        Map<String, String> data = new HashMap<>();
        data.put("yo", "666");
        data.put("ha", "222");
        annoFoo.setData(data);
        ObjectMapper om = new ObjectMapper();
        String res = om.writeValueAsString(annoFoo);
        System.out.println("res: " + res);
        // deserialize
        AnnoFoo toAnnoFoo = om.readValue(res, AnnoFoo.class);
        System.out.println("toAnnoFoo id: " + toAnnoFoo.getId());
        for (String key: toAnnoFoo.getData().keySet()) {
            System.out.println("key: " + key + ", val: " + toAnnoFoo.getData().get(key));
        }
    }

    public static void main(String[] args) throws Exception {
        ExecJson execJson = new ExecJson();
        System.out.println("=== basic use ===");
        execJson.basicUse();
        System.out.println("=== collection use ===");
        execJson.collectionUse();
        System.out.println("=== nested use ===");
        execJson.nestedUse();
        System.out.println("=== annotation use ===");
        execJson.annotationUse();
    }
}
