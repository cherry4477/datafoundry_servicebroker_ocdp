package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client;

import java.util.List;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.BaseRangerPolicy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.HBaseRangerPolicy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.HDFSRangerPolicy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.HiveRangerPolicy;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Java Client for manipulate Apache Ranger.
 *
 * @author whitebai1986@gmail.com
 *
 */
public class rangerClient {

    private HttpClient httpClient;
    private HttpClientContext context;
    private URI baseUri;

    static final Gson gson = new GsonBuilder().create();

    public rangerClient(String uri, String username, String password){

        if(! uri.endsWith("/")){
            uri += "/";
        }
        this.baseUri = URI.create(uri);

        this.httpClient = HttpClientBuilder.create().build();

        HttpHost targetHost = new HttpHost(this.baseUri.getHost(), 6080, "http");
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM),
                new UsernamePasswordCredentials(username, password));
        AuthCache authCache = new BasicAuthCache();
        authCache.put(targetHost, new BasicScheme());
        HttpClientContext context = HttpClientContext.create();
        context.setCredentialsProvider(provider);
        context.setAuthCache(authCache);
        this.context = context;
    }

    public String getPolicy(String policyID){
        String policyDef = null;
        URI uri = buildPolicyUri("service/public/api/policy", policyID, "");
        HttpGet request = new HttpGet(uri);
        try{
            HttpResponse response = this.httpClient.execute(request, this.context);
            if(response.getStatusLine().getStatusCode() == 200){
                policyDef = EntityUtils.toString(response.getEntity());
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return policyDef;
    }

    private String createPolicy(BaseRangerPolicy policy){
        String newPolicyString = null;
        String policyDef = gson.toJson(policy);
        URI uri = buildPolicyUri("service/public/api/policy", "", "");
        HttpPost request = new HttpPost(uri);
        StringEntity entity = new StringEntity(policyDef, HTTP.UTF_8);
        entity.setContentType("application/json");
        request.setEntity(entity);
        try{
            HttpResponse response = this.httpClient.execute(request, this.context);
            if(response.getStatusLine().getStatusCode() == 200)
            {
                newPolicyString = EntityUtils.toString(response.getEntity(),"UTF-8");
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return newPolicyString;
    }

    public String createHDFSPolicy(String policyName, String resourcePath, String description,
                                   String repositoryName, String repositoryType, List<String> groupList,
                                   List<String> userList, List<String> permList){
        String policyId = null;
        HDFSRangerPolicy rp = new HDFSRangerPolicy(policyName, "", resourcePath, description,
                repositoryName, repositoryType, true, true, true);
        rp.addPermToPolicy(groupList, userList, permList);
        String newPolicyString = this.createPolicy(rp);
        if (newPolicyString != null){
            HDFSRangerPolicy newPolicyObj = gson.fromJson(newPolicyString, HDFSRangerPolicy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        return policyId;
    }

    public String createHBasePolicy(String policyName, String tables, String columnFamilies, String columns,
                                    String description, String repositoryName, String repositoryType, List<String> groupList,
                                    List<String> userList, List<String> permList){
        String policyId = null;
        HBaseRangerPolicy rp = new HBaseRangerPolicy(policyName, "", tables, columnFamilies, columns,
                description, repositoryName, repositoryType, true, true, true);
        rp.addPermToPolicy(groupList, userList, permList);
        String newPolicyString = this.createPolicy(rp);
        if (newPolicyString != null){
            HBaseRangerPolicy newPolicyObj = gson.fromJson(newPolicyString, HBaseRangerPolicy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        return policyId;
    }

    public String createHivePolicy(String policyName, String databases, String tables, String columns,
                                   String description, String repositoryName, String repositoryType, List<String> groupList,
                                   List<String> userList, List<String> permList){
        String policyId = null;
        HiveRangerPolicy rp = new HiveRangerPolicy(policyName, "", databases, tables, columns,
                description, repositoryName, repositoryType, true, true, true);
        rp.addPermToPolicy(groupList, userList, permList);
        String newPolicyString = this.createPolicy(rp);
        if (newPolicyString != null){
            HiveRangerPolicy newPolicyObj = gson.fromJson(newPolicyString, HiveRangerPolicy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        return policyId;
    }

    public boolean removePolicy(String policyID){
        boolean status = false;
        URI uri = buildPolicyUri("service/public/api/policy", policyID, "");
        HttpDelete request = new HttpDelete(uri);
        try{
            HttpResponse response = this.httpClient.execute(request, this.context);
            status = (response.getStatusLine().getStatusCode() == 204);
        }catch (IOException e){
            e.printStackTrace();
        }
        return status;
    }

    public boolean updatePolicy(String policyID, String policyUpdateDef){
        boolean status = false;
        URI uri = buildPolicyUri("service/public/api/policy/" + policyID, "", "");
        HttpPut request = new HttpPut(uri);
        StringEntity entity = new StringEntity(policyUpdateDef, HTTP.UTF_8);
        entity.setContentType("application/json");
        request.setEntity(entity);
        try{
            HttpResponse response = this.httpClient.execute(request, this.context);
            status = (response.getStatusLine().getStatusCode() == 200);
        }catch (IOException e){
            e.printStackTrace();
        }
        return status;
    }

    private URI buildPolicyUri(String prefix, String key, String suffix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix);
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        for (String token : Splitter.on('/').split(key)) {
            sb.append("/");
            sb.append(urlEscape(token));
        }
        sb.append(suffix);

        URI uri = this.baseUri.resolve(sb.toString());
        return uri;
    }

    protected static String urlEscape(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException();
        }
    }
}
