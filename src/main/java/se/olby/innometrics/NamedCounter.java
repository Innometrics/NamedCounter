package se.olby.innometrics;
 
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.swagger.annotations.Api;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Api
@Path("/")
@Singleton
public class NamedCounter {
    //A SkipListMap let us navigate without blocking. Used for the list method
    private final ConcurrentNavigableMap<String, Integer> counters = new ConcurrentSkipListMap<>();

    private final JsonFactory jfactory = new JsonFactory();

    /** List counters **/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response list() {
        //Stream the content to reduce memory footprint.
        //This is directly mapped to the live Map giving possible memory usage of O(1)
        StreamingOutput stream = os -> {
            JsonGenerator jsonGenerator = jfactory.createGenerator(os);
            jsonGenerator.writeStartObject();
            for (String key: counters.keySet()) {
                Integer value = counters.get(key);
                //The underlying map may have changed since we retrieved the key
                if (value != null) {
                    jsonGenerator.writeNumberField(key, value);
                }
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.close();
        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    /** Read a counter **/
    @GET
    @Path("{counter}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response read(@PathParam("counter") String counter) {
        Integer value = counters.get(counter);
        if(value == null)
            return Response.status(Response.Status.NOT_FOUND).build();
        return Response.ok(value).build();
    }

    /** Init a counter **/
    @PUT
    @Path("{counter}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response init(@PathParam("counter") String counter) {
        if(counter == null || counter.trim().length() == 0)
            return Response.status(Response.Status.BAD_REQUEST).build();

        Integer value = counters.putIfAbsent(counter, 0);
        if(value == null || value == 0) //Accept 0 to make PUT idempotent
            return Response.status(Response.Status.CREATED).build();

        return Response.status(Response.Status.CONFLICT).build();
    }

    /** Delete a counter **/ 
    @DELETE
    @Path("{counter}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@PathParam("counter") String counter) {
        if(counter == null || counter.trim().length() == 0)
            return Response.status(Response.Status.BAD_REQUEST).build();

        if(counters.remove(counter) == null)
            return Response.status(Response.Status.NOT_FOUND).build();

        return Response.ok().build();
    }

    /** Delete a counter **/
    @DELETE
    @Path("{counter}/{condition}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteCAS(@PathParam("counter") String counter, @PathParam("condition") int condition) {
        if(counter == null || counter.trim().length() == 0)
            return Response.status(Response.Status.BAD_REQUEST).build();

        //Lock free retry, needed to atomically detect difference between missing and invalid condition
        for(;;) {
            Integer value = counters.get(counter);
            if(value == null)
                return Response.status(Response.Status.NOT_FOUND).build();
            if(value != condition)
                return Response.status(Response.Status.EXPECTATION_FAILED).build();
            //Spin iff backing value has been concurrently changed after the above check and before remove is executed
            if(counters.remove(counter, condition))
                return Response.ok().build();
        }
    }

    /** Increment a counter and returns the value after to the increment, CAS semantics can be implemented with an extra parameter (old valus)**/
    @POST
    @Path("{counter}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response increment(@PathParam("counter") String counter) {
        if(counter == null || counter.trim().length() == 0)
            return Response.status(Response.Status.BAD_REQUEST).build();

        Integer value = counters.computeIfPresent(counter, (key, oldValue) -> oldValue + 1);
        if(value == null)
            return Response.status(Response.Status.NOT_FOUND).build();

        return Response.ok(value).build();
    }

    /** Increment a counter and returns the value after to the increment, CAS semantics can be implemented with an extra parameter (old valus)**/
    @POST
    @Path("{counter}/{condition}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response incrementCAS(@PathParam("counter") String counter, @PathParam("condition") int condition) {
        if(counter == null || counter.trim().length() == 0)
            return Response.status(Response.Status.BAD_REQUEST).build();

        //Lock free retry, needed to atomically detect difference between missing and invalid condition
        for(;;) {
            Integer value = counters.get(counter);
            if(value == null)
                return Response.status(Response.Status.NOT_FOUND).build();
            if(value != condition)
                return Response.status(Response.Status.EXPECTATION_FAILED).build();
            //Spin iff backing value has been concurrently changed after the above check and before replace is executed
            if(counters.replace(counter, condition, condition + 1))
                return Response.ok(condition + 1).build();
        }
    }

}
