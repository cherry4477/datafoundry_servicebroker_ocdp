package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.http.HttpHost;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Map;

/**
 * Created by Aaron on 16/7/26.
 */
public class yarnClient{

    private CloseableHttpClient httpClient;
    private HttpClientContext context;
    private URI baseUri;

    private String totalMemory;
    private String availableMemory;
    private String allocateMemory;

    public yarnClient(String uri, String username, String password) {
        if(! uri.endsWith("/")){
            uri += "/";
        }
        this.baseUri = URI.create(uri);

        this.httpClient = HttpClientBuilder.create().build();

        HttpHost targetHost = new HttpHost(this.baseUri.getHost(), 8088, "http");
 //       CredentialsProvider provider = new BasicCredentialsProvider();
//        provider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM),
//                new UsernamePasswordCredentials(username, password));
        AuthCache authCache = new BasicAuthCache();
        authCache.put(targetHost, new BasicScheme());
        HttpClientContext context = HttpClientContext.create();
//        context.setCredentialsProvider(provider);
        context.setAuthCache(authCache);
        this.context = context;
    }

    public void getClusterMetrics(){

        URI uri = buildUri("","","ws/v1/cluster/metrics");

        HttpGet request = new HttpGet(uri);

        String jsonStr = excuteRequest(request);

        this.allocateMemory = getMetrics("allocatedMB",jsonStr);

        this.availableMemory = getMetrics("availableMB",jsonStr);

        this.totalMemory = String.valueOf(Double.parseDouble(this.allocateMemory) + Double.parseDouble(this.availableMemory));

    }

    private String getMetrics(String key, String jsonStr){

        String value = null;

        try{
            Map<?,?> response = null;
            Gson gson = new Gson();
            java.lang.reflect.Type type = new com.google.gson.reflect.TypeToken<Map<?, ?>>() {
            }.getType();
            response = gson.fromJson(jsonStr, type);

            LinkedTreeMap<?,?> clusterMetrics = (LinkedTreeMap<?, ?>) response.get("clusterMetrics");
            value = String.valueOf(clusterMetrics.get(key));
        }catch (Exception e){
            e.printStackTrace();
        }
        return value;
    }

    public String getTotalMemory(){ return this.totalMemory;}
    public String getAvailableMemory(){ return  this.availableMemory;}
    public String getAllocateMemory(){ return this.allocateMemory;}

    private String excuteRequest(HttpUriRequest request)
    {
        String responseDef = null;
        try{
            CloseableHttpResponse response = this.httpClient.execute(request, this.context);
            if(response.getStatusLine().getStatusCode() == 200){
                responseDef = EntityUtils.toString(response.getEntity());
            }
            response.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return responseDef;
    }

    private URI buildUri(String prefix, String key, String suffix) {
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
