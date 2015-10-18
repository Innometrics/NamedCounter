package se.olby.innometrics;
 
import javax.ws.rs.GET;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import io.swagger.annotations.Api;

@Api
@Path("/")
public class NamedCounter {

    /** List counters **/ 
    @GET
    @Path("/")
    @Produces(MediaType.TEXT_PLAIN)
    public String list() {
        return "";
    }

    /** Init a counter **/ 
    @GET
    @Path("{counter}")
    @Produces(MediaType.TEXT_PLAIN)
    public String init(@PathParam("counter") String counter) {
        return counter;
    }

    /** Delete a counter **/ 
    @DELETE
    @Path("{counter}")
    @Produces(MediaType.TEXT_PLAIN)
    public String delete(@PathParam("counter") String counter) {
        return counter;
    }

    /** Increment a counter and returns the value prior to the increment, CAS sematics can be implementet with an extra parameter (old valus)**/ 
    @POST
    @Path("{counter}")
    @Produces(MediaType.TEXT_PLAIN)
    public String increment(@PathParam("counter") String counter) {
        return counter;
    }

}
