package org.apache.zeppelin.realm;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.io.IOException;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.zeppelin.server.ZeppelinServer;
import org.apache.zeppelin.user.UserCredentials;
import org.apache.zeppelin.user.UsernamePassword;
import org.eclipse.jetty.util.security.Credential;

import javax.inject.Inject;

public class MetatronRealm extends AuthorizingRealm  {
  String authUrl; // http(s)://localhost:8180/oauth/token
  Gson gson = new Gson();
  CloseableHttpClient httpClient = HttpClients.createDefault();

  public MetatronRealm() {
    super();
  }

  @Override
  protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principalCollection) {
    return null;
  }

  @Override
  protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken authenticationToken) throws AuthenticationException {
    UsernamePasswordToken token = (UsernamePasswordToken) authenticationToken;
    AuthResponse authResp;
    try {
      authResp = auth(token.getUsername(), new StringBuilder().append(token.getPassword()).toString());
    } catch (IOException e) {
      throw new AuthenticationException(e);
    }

    if (!authResp.isAuthenticated()) {
      throw new AuthenticationException("Invalid id or password");
    }

    return new SimpleAuthenticationInfo(token.getUsername(), token.getPassword(), MetatronRealm.class.getName());
  }

  public class AuthResponse {
    String access_token;
    String token_type;
    long expires_in;
    String scope;
    String jti;

    public boolean isAuthenticated() {
      return access_token != null;
    }

    public String getAccess_token() {
      return access_token;
    }

    public String getToken_type() {
      return token_type;
    }

    public long getExpires_in() {
      return expires_in;
    }

    public String getScope() {
      return scope;
    }

    public String getJti() {
      return jti;
    }
  }

  AuthResponse auth(String username, String password) throws IOException {
    HttpPost post = new HttpPost(authUrl);
    post.setHeader("Content-Type", "application/x-www-form-urlencoded");
    post.setHeader("Authorization", "Basic cG9sYXJpc19jbGllbnQ6cG9sYXJpcw==");

    post.setEntity(new UrlEncodedFormEntity(ImmutableList.of(
            new BasicNameValuePair("grant_type", "password"),
            new BasicNameValuePair("scope", "write"),
            new BasicNameValuePair("username", username),
            new BasicNameValuePair("password", password)
    )));
    CloseableHttpResponse resp = httpClient.execute(post);

    String result = IOUtils.toString(resp.getEntity().getContent(), Charsets.UTF_8);
    AuthResponse authResp = gson.fromJson(result, AuthResponse.class);

    // put user credential, so interpreter can read this information
    UserCredentials userCredentials = new UserCredentials();
    userCredentials.putUsernamePassword("token", new UsernamePassword(username, authResp.access_token));
    ZeppelinServer.notebook.getCredentials().putUserCredentials(username, userCredentials);

    return authResp;
  }

  public void setAuthUrl(String authUrl) {
    this.authUrl = authUrl;
  }
}
