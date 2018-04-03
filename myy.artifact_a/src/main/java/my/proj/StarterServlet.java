@WebServlet(name = "Starter", value = "/")
public class DataflowSchedulingServlet extends HttpServlet {

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    StarterPipeline.run();
  }
}
