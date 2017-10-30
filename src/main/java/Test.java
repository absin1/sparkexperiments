import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {
    public static void main(String[] args) throws SQLException {
         clusterPaginationEntry();
    }

    private static void clusterPaginationEntry() throws SQLException {
        String sql = "SELECT id, url, cluster_type, cluster_name FROM public.job_listing_clusters; ";
        DB db = new DB();
        ResultSet runSql = db.runSql(sql);
        while (runSql.next()) {
            int cluster_id = runSql.getInt(1);
            String url = runSql.getString(2);
            System.out.println(cluster_id+">>>>"+url);
        }
    }
}