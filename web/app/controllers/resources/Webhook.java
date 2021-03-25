
package controllers.resources;

import play.mvc.Controller;
import play.mvc.Result;
import org.lobid.resources.run.AlmaMarcXml2lobidJsonEs;
import play.Logger;

/**
 * Simple webhook listener starting update process for the Alma ETL.
 * 
 * @author Pascal Christoph (dr0i)
 */
public class Webhook extends Controller{

  private static final String FILE="/data/other/datenportal/export/alma/prod/update.xml.bgzf";
  private static final String INDEX_NAME="almawebhooktestupdate";
  private static final String INDEX_ALIAS_SUFFIX="NOALIAS";
  private static final String ES_NODE="weywot5.hbz-nrw.de";
  private static final String ES_CLUSTER_NAME="weywot";
  private static final String UPDATE_NEWEST_INDEX="exact";
  private static final String MORPH_FILENAME="alma.xml";
 /** 
  * @return 200 ok or 403 forbidden response depending on token
  * @throws IOException if data files cannot be read
 */
  public static Result updateAlma(final String token) {
    if ( !token.equalsIgnoreCase("123")) {
        return forbidden("Wrong token. Declining to update.");
}
try {
      System.out.println("doit");
    AlmaMarcXml2lobidJsonEs.main(FILE, INDEX_NAME, INDEX_ALIAS_SUFFIX, ES_NODE, ES_CLUSTER_NAME, UPDATE_NEWEST_INDEX, MORPH_FILENAME);
} catch (Exception e) {
Logger.error("Transformation failed", e);
}
      return ok("ETL updates");
  }
}
