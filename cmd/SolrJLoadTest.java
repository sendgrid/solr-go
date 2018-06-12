


import java.util.ArrayList;
import java.util.Collection;

import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import java.util.UUID;

public class SolrJLoadTest {
	public static void main(String[] argv) {
		System.out.println("-------- Oracle SolrJ Testing ------");

                SolrJLoadTest service = new SolrJLoadTest();

                service.doTest();

                System.exit(0);
	}
    public static String getTimestamp(){
            return( getTimestamp(java.util.Calendar.getInstance().getTime()) );
        }

        public static String getTimestamp(java.util.Date date){
            String result = "";

            java.text.SimpleDateFormat out = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            result = out.format(date);

            return( result );
        }

    public void doTest(){
        org.apache.solr.client.solrj.impl.CloudSolrClient server = null;

        try{
            server =  new CloudSolrClient.Builder().withZkHost("zk:2181").withZkChroot("/solr").build();
            server.setDefaultCollection("solrtest");

            int maxRows = 1000*20;
            int maxTrys = 20;
            int counter = 0;
            String lastname = UUID.randomUUID().toString();
            for(;counter < maxRows;counter++){
                String uuid = UUID.randomUUID().toString();
                boolean failedSend = false;
                SolrInputDocument doc = new SolrInputDocument();
                doc.setField("id","customer123_yzrb!ID" + uuid);
                doc.setField("email","email"+uuid);
                doc.setField("last_name",lastname);
                doc.setField("first_name","fname"+uuid);

                doc.setField("created_at",getTimestamp());
                Collection<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();

                docList.add(doc);
                for(int j = 1;;j++){
                    try{

                        UpdateResponse res = server.add(docList);
                        System.out.println("counter = "+counter+" response = " + res.getStatus());
                        if( res.getStatus() == 0 ){
                            break;
                        }
                        else if( j > maxTrys) {
                            failedSend = true;
                            break;
                        }
                    }catch(Exception e){
                        System.out.println(e.toString());

                    }

                }

                if( failedSend ){
                    System.out.println("failed send at: " + counter);
                }

            }

            System.out.println("added: " + counter);
        }
        catch(Exception e){
            System.out.println("failed: " + e.toString());
        }
        finally {
            if( server != null ){
                try{
                    // server.shutdown();
                }
                catch(Exception e){
                    System.out.println("unable to close: " + e.toString());
                }
            }
        }
    }


}