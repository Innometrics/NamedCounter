package se.olby.innometrics;
 
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

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
    private static final String COUNTERS_URL = "counters/";

    private static final String MISSING = "No counter with matching name found";
    private static final String CAS_EXPECTATION = "Current value is different from given condition";
    private static final String SUBZERO_CONDITION = "Condition can never be less than zero";
    private static final String MESSAGE_200 = "Success";
    private static final String CONFLICT = "A counter with the given name already exists";

    //A SkipListMap let us navigate without blocking. Used for the list method
    private final ConcurrentNavigableMap<String, Integer> counters = new ConcurrentSkipListMap<>();

    private final JsonFactory jfactory = new JsonFactory();

    //Used to return Json strings since plain Strings are not serialized to json strings
    private static class Error {
        private final String message;
        private Error(String message) {
            this.message = message;
        }
        static Error of(String message) {
            return new Error(message);
        }

        @JsonValue
        public String getMessage() {
            return message;
        }
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    @ApiOperation(
            value = "HTML documentation"
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = MESSAGE_200),
            })
    public Response readme() {
        return Response
                .ok(ClassLoader.getSystemResourceAsStream("readme.html"))
                .build();
    }

    @GET
    @Path(COUNTERS_URL)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Lists all counters as name value pairs",
            produces = "A json object with fields for each counter with the integer value as value"
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = MESSAGE_200),
            })
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

        return Response
                .ok()
                .entity(stream)
                .build();
    }

    @GET
    @Path(COUNTERS_URL + "{counter}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Read the value of the counter with the given name",
            produces = "The integer value or descriptive error message as a string"
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = MESSAGE_200),
                    @ApiResponse(code = 404, message = MISSING)
            })
    public Response read(@PathParam("counter") String counter) {
        Integer value = counters.get(counter);
        if(value == null)
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(Error.of(MISSING))
                    .build();

        return Response
                .ok(value)
                .build();
    }

    @PUT
    @Path(COUNTERS_URL + "{counter}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Initiates a counter with the given name to zero",
            produces = "Nothing or descriptive error message as a string"
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = MESSAGE_200),
                    @ApiResponse(code = 409, message = CONFLICT)
            })
    public Response init(@PathParam("counter") String counter) {
        Integer value = counters.putIfAbsent(counter, 0);
        if(value == null || value == 0) //Accept 0 to make PUT idempotent
            return Response
                    .status(Response.Status.CREATED)
                    .build();

        return Response
                .status(Response.Status.CONFLICT)
                .entity(Error.of(CONFLICT))
                .build();
    }

    /** Delete a counter **/ 
    @DELETE
    @Path(COUNTERS_URL + "{counter}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Deletes a named counter",
            produces = "The integer value of the deleted counter or a descriptive error message as a string"
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = MESSAGE_200),
                    @ApiResponse(code = 404, message = MISSING)
            })
    public Response delete(@PathParam("counter") String counter) {
        Integer value = counters.remove(counter);
        if(value == null)
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(Error.of(MISSING))
                    .build();

        return Response
                .ok(value)
                .build();
    }

    @DELETE
    @Path(COUNTERS_URL + "{counter}/{condition}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Deletes a named counter",
            notes = "The given conditional value must match the current value of counter. This provide a barrier to " +
                    "detect concurrent updates",
            produces = "The integer value of the deleted counter or a descriptive error message as a string"
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = MESSAGE_200),
                    @ApiResponse(code = 400, message = SUBZERO_CONDITION),
                    @ApiResponse(code = 417, message = CAS_EXPECTATION),
                    @ApiResponse(code = 404, message = MISSING)
            })
    public Response deleteCAS(@PathParam("counter") String counter, @PathParam("condition") int condition) {
        if(condition < 0)
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity(Error.of(SUBZERO_CONDITION))
                    .build();

        //Lock free retry, needed to atomically detect difference between missing and invalid condition
        for(;;) {
            Integer value = counters.get(counter);
            if(value == null)
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(Error.of(MISSING))
                        .build();
            if(value != condition)
                return Response
                        .status(Response.Status.EXPECTATION_FAILED)
                        .entity(Error.of(CAS_EXPECTATION))
                        .build();
            //Spin iff backing value has been concurrently changed after the above check and before remove is executed
            if(counters.remove(counter, condition))
                return Response
                        .ok(value)
                        .build();
        }
    }

    @POST
    @Path(COUNTERS_URL + "{counter}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Increments a named counter",
            produces = "The incremented integer value or a descriptive error message as a string"
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = MESSAGE_200),
                    @ApiResponse(code = 404, message = MISSING)
            })
    public Response increment(@PathParam("counter") String counter) {
        Integer value = counters.computeIfPresent(counter, (key, oldValue) -> oldValue + 1);
        if(value == null)
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(Error.of(MISSING))
                    .build();

        return Response
                .ok(value)
                .build();
    }

    @POST
    @Path(COUNTERS_URL + "{counter}/{condition}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Increments a named counter",
            notes = "The given conditional value must match the current value of counter. This provide a barrier to " +
                    "detect concurrent updates",
            produces = "The incremented integer value or a descriptive error message as a string"
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = MESSAGE_200),
                    @ApiResponse(code = 400, message = SUBZERO_CONDITION),
                    @ApiResponse(code = 417, message = CAS_EXPECTATION),
                    @ApiResponse(code = 404, message = MISSING)
            })
    public Response incrementCAS(@PathParam("counter") String counter, @PathParam("condition") int condition) {
        if(condition < 0)
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity(Error.of(SUBZERO_CONDITION))
                    .build();

        //Lock free retry, needed to atomically detect difference between missing and invalid condition
        for(;;) {
            Integer value = counters.get(counter);
            if(value == null)
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(Error.of(MISSING))
                        .build();
            if(value != condition)
                return Response
                        .status(Response.Status.EXPECTATION_FAILED)
                        .entity(Error.of(CAS_EXPECTATION))
                        .build();
            //Spin iff backing value has been concurrently changed after the above check and before replace is executed
            if(counters.replace(counter, condition, condition + 1))
                return Response
                        .ok(condition + 1)
                        .build();
        }
    }

}
