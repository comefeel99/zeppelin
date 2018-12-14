package org.apache.zeppelin.metatron.client;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.zeppelin.metatron.message.AuthResponse;
import org.apache.zeppelin.metatron.message.DataRequest;
import org.apache.zeppelin.metatron.message.DataResponse;
import org.apache.zeppelin.metatron.message.Datasource;
import org.apache.zeppelin.metatron.message.DatasourceDetail;
import org.apache.zeppelin.metatron.message.DatasourceRequest;
import org.apache.zeppelin.metatron.message.DatasourcesResponse;
import org.apache.zeppelin.metatron.message.Filter;
import org.apache.zeppelin.metatron.message.Limits;
import org.apache.zeppelin.metatron.message.Projection;

/**
 * Metatron Http client
 */
public class MetatronClient {

  private final String baseUrl;
  private Gson gson;
  private String accessToken;
  CloseableHttpClient httpClient = HttpClients.createDefault();

  enum RequestPath {
    OAUTH_TOKEN("/oauth/token"),
    API_DATASOURCES("/api/datasources"),
    API_DATASOURCES_QUERY("/api/datasources/query/search");

    String path;
    RequestPath(String path) {
      this.path = path;
    }

    public String path() {
      return path;
    }
  }

  public MetatronClient(String baseUrl) throws IOException {
    this.baseUrl = baseUrl;
    gson = new GsonBuilder()
            .registerTypeAdapter(Date.class, new DateDeserializer())
            .create();

    auth();
  }



  public List<Datasource> showDatasources() throws IOException {
    String url = baseUrl + "/api/datasources";

    HttpGet get = httpGet(RequestPath.API_DATASOURCES);
    CloseableHttpResponse resp = httpClient.execute(get);
    DatasourcesResponse dsResp = readResponse(resp, DatasourcesResponse.class);
    return dsResp.getDatasources();
  }

  public DatasourceDetail showDatasource(String dsName) throws IOException {
    Datasource ds = getDatasourceByName(dsName);
    if (ds == null) {
      return null;
    }

    String url = String.format("%s/%s?projection=forDetailView", RequestPath.API_DATASOURCES.path(), ds.getId());

    HttpGet get = httpGet(url);
    CloseableHttpResponse resp = httpClient.execute(get);
    return readResponse(resp, DatasourceDetail.class);
  }

  private Datasource getDatasourceByName(String name) throws IOException {
    List<Datasource> datasources = showDatasources();
    for (Datasource ds : datasources) {
      if (name.equals(ds.getName())) {
        return ds;
      }
    }
    return null;
  }

  public DataResponse getData(
          String datasourceName,
          List<Filter> filters,
          List<Projection> projections,
          Limits limits) throws IOException {
    Datasource ds = getDatasourceByName(datasourceName);
    if (ds == null) {
      throw new IOException("Data source not found");
    }

    DataRequest dr = new DataRequest(
            new DatasourceRequest(ds.getName(), "default", false),
            filters,
            projections,
            limits,
            true
    );

    return getData(dr);
  }

  public DataResponse getData(DataRequest dataRequest) throws IOException {
    HttpPost post = httpPost(RequestPath.API_DATASOURCES_QUERY);
    try {
      String request = gson.toJson(dataRequest);
      post.setEntity(new StringEntity(request));
    } catch (UnsupportedEncodingException e) {
      throw new IOException(e);
    }

    CloseableHttpResponse resp = httpClient.execute(post);
    return readResponse(resp, DataResponse.class);
  }


  /**
   * Get authentication token
   * TODO: delete this method after after proper auth integration
   * @return
   */
  AuthResponse auth() throws IOException {
    int protoPos = baseUrl.indexOf("://");
    String url = "";

    if (protoPos > 0) {
      protoPos += "://".length();
      url = baseUrl.substring(0, protoPos);
    }
    url += "polaris_trusted:secret@" + baseUrl.substring(protoPos) + RequestPath.OAUTH_TOKEN.path();

    HttpPost post = new HttpPost(url);

    post.setEntity(new UrlEncodedFormEntity(ImmutableList.of(
            new BasicNameValuePair("grant_type", "client_credentials")
    )));
    CloseableHttpResponse resp = httpClient.execute(post);

    String result = IOUtils.toString(resp.getEntity().getContent(), Charsets.UTF_8);
    AuthResponse authResp = gson.fromJson(result, AuthResponse.class);
    accessToken = authResp.getAccess_token();
    return authResp;
  }

  HttpGet httpGet(RequestPath path) {
    return httpGet(path.path());
  }

  HttpGet httpGet(String path) {
    String url = baseUrl + path;
    HttpGet get = new HttpGet(url);
    setToken(get);
    return get;
  }

  HttpPost httpPost(RequestPath path) {
    return httpPost(path.path());
  }

  HttpPost httpPost(String path) {
    String url = baseUrl + path;
    HttpPost post = new HttpPost(url);
    setToken(post);
    return post;
  }

  <T> T readResponse(HttpResponse resp, Class<T> type) throws IOException {
    String result = IOUtils.toString(resp.getEntity().getContent(), Charsets.UTF_8);
    return gson.fromJson(result, type);
  }

  void setToken(HttpUriRequest req) {
    req.setHeader("Content-Type", "application/json");
    req.setHeader("Authorization", "bearer " + accessToken);
  }

}
