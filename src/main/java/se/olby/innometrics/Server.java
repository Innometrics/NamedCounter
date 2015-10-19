package se.olby.innometrics;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

public class Server {
    public static void main(String[] args) throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        int port = Integer.valueOf(System.getenv("PORT"));
        org.eclipse.jetty.server.Server jettyServer = new org.eclipse.jetty.server.Server(port);
        jettyServer.setHandler(context);
 
        ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);
 
        jerseyServlet.setInitParameter("javax.ws.rs.Application", NamedCounterApplication.class.getCanonicalName());
 
        try {
            jettyServer.start();
            jettyServer.join();
        } finally {
            jettyServer.destroy();
        }
    }
}
