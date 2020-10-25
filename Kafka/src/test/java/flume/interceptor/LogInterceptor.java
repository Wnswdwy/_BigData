package flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Flume  Interceptor
 */
public class LogInterceptor implements Interceptor {

    Logger logger = LoggerFactory.getLogger(LogInterceptor.class);

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取event的body  和 headers
        String body = new String(event.getBody());
        Map<String, String> headers = event.getHeaders();

        //判断
        if(body.contains("user")){
            headers.put("topic","user");
            logger.info("user:" + headers);

        }else if (body.contains("cart")){
            headers.put("topic","cart");
            logger.info("cart:" + headers);
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements  Builder{

        @Override
        public Interceptor build() {
            return new  LogInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
