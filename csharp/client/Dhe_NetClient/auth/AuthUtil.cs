//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Google.Protobuf;
using Io.Deephaven.Proto.Auth;

namespace Deephaven.Dhe_NetClient;

public static class AuthUtil {
  public static Token ProtoFromAuthToken(AuthToken authToken) {
    var tokenProto = new Token {
      TokenId = authToken.TokenId,
      Service = authToken.Service,
      IpAddress = authToken.OriginIpAddressBytes,
      UserContext = new Io.Deephaven.Proto.Auth.UserContext {
        AuthenticatedUser = authToken.UserContext.User,
        EffectiveUser = authToken.UserContext.EffectiveUser
      }
    };
    return tokenProto;
  }

  public static AuthToken AuthTokenFromProto(Token token) {
    var uc = new UserContext(token.UserContext.AuthenticatedUser,
      token.UserContext.EffectiveUser);
    return new AuthToken(token.TokenId, token.Service, uc, token.IpAddress);
  }

  public static string AsBase64Proto(AuthToken authToken) {
    var tokenProto = ProtoFromAuthToken(authToken);
    var bytes = tokenProto.ToByteArray();
    return System.Convert.ToBase64String(bytes);
  }
}
