package org.apache.zeppelin.metatron.message;

public class AuthResponse {
  String access_token;
  String token_type;
  long expires_in;
  String scope;
  String jti;

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
