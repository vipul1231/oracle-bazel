package com.example.db;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 8/13/2021<br/>
 * Time: 9:45 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class DefaultObjectMapper {
    /**
     * This object is immutable! Because many parts of the code share this object, modifying it can cause unpredictable
     * far-reaching changes. Any attempt to modify it with {@link ObjectMapper#configure}, {@link
     * ObjectMapper#registerModule}, or similar methods will fail. If you need to configure it, you should use {@link
     * DefaultObjectMapper#create()} to create your own copy.
     */
    public static final ObjectMapper JSON = create();

    public static ObjectMapper create() {
        ObjectMapper mapper = new ObjectMapper();

        //mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new Jdk8Module());
        //mapper.registerModule(new PathModule());

        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);

        // Necessary for annotated views to remain private
        //mapper.setConfig(mapper.getSerializationConfig().withView(JsonViews.Public.class));

        return mapper;
    }
}