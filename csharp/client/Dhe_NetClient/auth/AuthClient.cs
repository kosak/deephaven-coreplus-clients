//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Google.Protobuf;
using Io.Deephaven.Proto.Auth;
using Io.Deephaven.Proto.Auth.Grpc;
using Grpc.Net.Client;
using Deephaven.Dh_NetClient;

namespace Deephaven.Dhe_NetClient;

public class AuthClient : IDisposable {
  public static AuthClient Connect(string descriptiveName, Credentials credentials,
    string target, ClientOptions options) {
    var channel = GrpcUtil.CreateChannel(target, options);
    var authApi = new AuthApi.AuthApiClient(channel);
    var uuid = System.Guid.NewGuid().ToByteArray();
    var clientId = ClientUtil.MakeClientId(descriptiveName, uuid);
    var (authCookie, deadline) = Authenticate(clientId, authApi, credentials);
    var result = new AuthClient(clientId, channel, authApi, authCookie, deadline);
    return result;
  }

  private static (byte[], long) Authenticate(ClientId clientId,
    AuthApi.AuthApiClient authApi, Credentials credentials) {
    // TODO(kosak): more credential types here
    if (credentials is not Credentials.PasswordCredentials pwc) {
      throw new Exception("Unexpected credentials type");
    }
    var req = new AuthenticateByPasswordRequest {
      ClientId = clientId,
      Password = pwc.Password,
      UserContext = new Io.Deephaven.Proto.Auth.UserContext {
        AuthenticatedUser = pwc.User,
        EffectiveUser = pwc.OperateAs
      }
    };

    var res = authApi.authenticateByPassword(req).Result;
    if (!res.Authenticated) {
      throw new Exception("Password authentication failed");
    }

    var cookie = res.Cookie.ToByteArray();
    return (cookie, res.CookieDeadlineTimeMillis);
  }

  private readonly ClientId _clientId;
  private readonly GrpcChannel _channel;
  private readonly AuthApi.AuthApiClient _authApi;

  /// <summary>
  /// These fields are all protected by a synchronization object
  /// </summary>
  private struct SyncedFields {
    public readonly object SyncRoot = new();
    public byte[] Cookie;
    public readonly Timer Keepalive;
    public bool Cancelled = false;

    public SyncedFields(byte[] cookie, Timer keepalive) {
      Cookie = cookie;
      Keepalive = keepalive;
    }
  }

  private SyncedFields _synced;

  private AuthClient(ClientId clientId, GrpcChannel channel, AuthApi.AuthApiClient authApi,
    byte[] cookie, long cookieDeadlineTimeMillis) {
    _clientId = clientId;
    _channel = channel;
    _authApi = authApi;
    var keepalive = new Timer(RefreshCookie);
    _synced = new SyncedFields(cookie, keepalive);
    var delayTime = CalcDelayTime(cookieDeadlineTimeMillis);
    keepalive.Change(delayTime, Timeout.InfiniteTimeSpan);
  }

  public void Dispose() {
    lock (_synced.SyncRoot) {
      if (_synced.Cancelled) {
        return;
      }
      _synced.Cancelled = true;
      _synced.Keepalive.Dispose();
    }

    _channel.Dispose();
  }

  internal AuthToken CreateToken(string forService) {
    GetTokenRequest request;
    lock (_synced.SyncRoot) {
      request = new GetTokenRequest {
        Service = forService,
        Cookie = ByteString.CopyFrom(_synced.Cookie)
      };
    }
    var response = _authApi.getToken(request);
    return AuthUtil.AuthTokenFromProto(response.Token);
  }

  private void RefreshCookie(object? _) {
    RefreshCookieRequest req;
    lock (_synced.SyncRoot) {
      if (_synced.Cancelled) {
        return;
      }
      req = new RefreshCookieRequest {
        Cookie = ByteString.CopyFrom(_synced.Cookie)
      };
    }

    var resp = _authApi.refreshCookie(req);
    var delayTime = CalcDelayTime(resp.CookieDeadlineTimeMillis);

    lock (_synced.SyncRoot) {
      // Empty Cookie means reuse same cookie with new deadline.
      if (resp.Cookie.Length != 0) {
        _synced.Cookie = resp.Cookie.ToByteArray();
      }
      _synced.Keepalive.Change(delayTime, Timeout.InfiniteTimeSpan);
    }
  }

  private static TimeSpan CalcDelayTime(long cookieDeadlineTimeMillis) {
    var deadline = DateTimeOffset.FromUnixTimeMilliseconds(cookieDeadlineTimeMillis);
    var delayMillis = (int)(Math.Max(0,
      (deadline - DateTimeOffset.Now).TotalMilliseconds) / 2);
    return TimeSpan.FromMilliseconds(delayMillis);
  }
}
