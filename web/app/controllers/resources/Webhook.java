
package controllers.resources;

import play.libs.F.Promise;
import play.mvc.Controller;
import play.mvc.Result;
import org.lobid.resources.run.AlmaMarcXml2lobidJsonEs;

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
  
  public static Promise<Result> updateAlma(final String token) {
    Promise<Result> result = null;
    if (token.equalsIgnoreCase("123")) {
      System.out.println("doit");
    AlmaMarcXml2lobidJsonEs.main(FILE, INDEX_NAME, INDEX_ALIAS_SUFFIX, ES_NODE, ES_CLUSTER_NAME, UPDATE_NEWEST_INDEX, MORPH_FILENAME);
    } else
      result = Promise.promise(() -> {
        return badRequest("Wrong token. Declining to update.");
      });
      return result;
  }
}
