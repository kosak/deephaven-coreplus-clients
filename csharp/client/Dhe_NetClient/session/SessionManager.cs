//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Text.Json;
using System.Threading.Channels;
using Deephaven.Dh_NetClient;
using Grpc.Core;
using Io.Deephaven.Proto.Controller;

namespace Deephaven.Dhe_NetClient;

public class SessionManager : IDisposable {
  private const string DefaultOverrideAuthority = "authserver";
  private const string DispatcherServiceName = "RemoteQueryProcessor";

  private class SessionInfo {
    public required string[] auth_host { get; set; }
    public required UInt16 auth_port { get; set; }
    public required string controller_host { get; set; }
    public required UInt16 controller_port { get; set; }
    public string? truststore_url { get; set; }
    public bool override_authorities { get; set; }
    public string? auth_authority { get; set; }
    public string? controller_authority { get; set; }
  }

  public static SessionManager FromUrl(string descriptiveName, Credentials credentials,
    string url, bool validateCertificate = true) {
    var json = GetUrl(url, validateCertificate);
    return FromJson(descriptiveName, credentials, json);
  }

  public static SessionManager FromJson(string descriptiveName, Credentials credentials,
    string json) {
    var info = JsonSerializer.Deserialize<SessionInfo>(json);
    if (info == null) {
      // Can this happen?
      throw new Exception("Deserialize returned null");
    }

    string? rootCerts = null;
    if (info.truststore_url != null) {
      // TODO(kosak): true, false, or pass through some parameter?
      rootCerts = GetUrl(info.truststore_url, false);
    }

    string? authAuthority = null;
    string? controllerAuthority = null;
    if (info.override_authorities) {
      authAuthority = info.auth_authority ?? DefaultOverrideAuthority;
      controllerAuthority = info.controller_authority ?? DefaultOverrideAuthority;
    }

    ArgumentNullException.ThrowIfNull(authAuthority, nameof(authAuthority));
    ArgumentNullException.ThrowIfNull(controllerAuthority, nameof(controllerAuthority));
    ArgumentNullException.ThrowIfNull(rootCerts, nameof(rootCerts));

    return Create(
      descriptiveName,
      credentials,
      info.auth_host[0], info.auth_port,
      authAuthority,
      info.controller_host, info.controller_port,
      controllerAuthority,
      rootCerts);
  }

  private static SessionManager Create(
    string descriptiveName, Credentials credentials,
    string authHost, UInt16 authPort, string authAuthority,
    string controllerHost, UInt16 controllerPort, string controllerAuthority,
    string rootCerts) {
    var (authTarget, authOptions) = SetupClientOptions(authHost, authPort,
      authAuthority, rootCerts);
    var authClient = AuthClient.Connect(descriptiveName, credentials, authTarget, authOptions);
    var (controllerTarget, controllerOptions) = SetupClientOptions(
      controllerHost, controllerPort,
      controllerAuthority, rootCerts);
    var controllerClient = ControllerClient.Connect(descriptiveName,
      controllerTarget, controllerOptions, authClient);
    return new SessionManager(descriptiveName, authClient, controllerClient,
      authAuthority, controllerAuthority, rootCerts);
  }

  private static (string, ClientOptions) SetupClientOptions(string host, UInt16 port,
    string? overrideAuthority, string rootCerts) {
    var target = $"{host}:{port}";
    var clientOptions = new ClientOptions {
      UseTls = true,
      TlsRootCerts = rootCerts
    };
    if (overrideAuthority != null) {
      // TODO(kosak): not supported in the library directly. Probably need to
      // follow the advice of https://github.com/grpc/grpc-dotnet/issues/1362
      // which may require refactoring the code in the core client.
      // clientOptions.AddStringOption(GRPC_SSL_TARGET_NAME_OVERRIDE_ARG, override_authority);
    }

    return (target, clientOptions);
  }

  private readonly string _logId;
  private readonly AuthClient _authClient;
  private readonly ControllerClient _controllerClient;
  private readonly string _authAuthority;
  private readonly string _controllerAuthority;
  private readonly string _rootCerts;

  private SessionManager(string logId, AuthClient authClient, ControllerClient controllerClient,
    string authAuthority, string controllerAuthority, string rootCerts) {
    _logId = logId;
    _authClient = authClient;
    _controllerClient = controllerClient;
    _authAuthority = authAuthority;
    _controllerAuthority = controllerAuthority;
    _rootCerts = rootCerts;
  }

  public void Dispose() {
    _controllerClient.Dispose();
    _authClient.Dispose();
  }

  public DndClient AddQueryAndConnect(PersistentQueryConfigMessage pqConfig) {
    var serial = AddQueryAndStart(pqConfig);
    var client = ConnectToPqById(serial, true);
    return client;
  }

  public Int64 AddQueryAndStart(PersistentQueryConfigMessage pqConfig) {
    var serial = _controllerClient.AddQuery(pqConfig);
    _controllerClient.RestartQuery(serial);
    return serial;
  }

  public PersistentQueryConfigMessage MakeTempPqConfig(string pqName) {
    return _controllerClient.MakeTempPqConfig(pqName);
  }

  public DndClient ConnectToPqByName(string pqName, bool removeOnClose) {
    var (pqSerial, client) = FindPqAndConnect(dict => {
      var result = dict.Values.FirstOrDefault(i => i.Config.Name == pqName);
      if (result == null) {
        throw new Exception($"pq name='{pqName}' not found");
      }
      return result;
    });
    return DndClient.Create(pqSerial, this, client, removeOnClose);
  }

  public DndClient ConnectToPqById(Int64 pqSerial, bool removeOnClose) {
    var (_, client) = FindPqAndConnect(dict => {
      if (!dict.TryGetValue(pqSerial, out var result)) {
        throw new Exception($"pqSerial='{pqSerial}' not found");
      }
      return result;
    });
    return DndClient.Create(pqSerial, this, client, removeOnClose);
  }

  private (Int64, Client) FindPqAndConnect(
    Func<IReadOnlyDictionary<Int64, PersistentQueryInfoMessage>, PersistentQueryInfoMessage> filter) {
    using var subscription = _controllerClient.Subscribe();
    if (!subscription.Current(out var version, out var configMap)) {
      throw new Exception("Controller subscription has closed");
    }

    var info = filter(configMap);
    var pqSerial = info.Config.Serial;
    var pqName = info.Config.Name;

    while (true) {
      // It may make sense to have the ability to provide a timeout
      // in the future; absent a terminal or running state being found or
      // the subscription closing, as is this will happily run forever.
      var status = info.State.Status;
      if (ControllerClient.IsTerminal(status)) {
        throw new Exception($"pqName='{pqName}', pqSerial={pqSerial} " +
          $"is in terminal state={info.State.Status}");
      }

      if (ControllerClient.IsRunning(status)) {
        break;
      }

      if (!subscription.Next(version) ||
          !subscription.Current(out version, out configMap)) {
        throw new Exception("Controller subscription has closed");
      }

      if (!configMap.TryGetValue(pqSerial, out info)) {
        throw new Exception($"pqName='{pqName}', pqSerial={pqSerial} " +
          $"is no longer available");
      }
    }

    return ConnectToPq(info);
  }

  private (Int64, Client) ConnectToPq(PersistentQueryInfoMessage infoMsg) {
    var url = infoMsg.State.ConnectionDetails.GrpcUrl;
    var pqSerial = infoMsg.Config.Serial;

    var pqStrForErr = $"pq_name='{infoMsg.Config.Name}', pq_serial={pqSerial}";

    if (url.IsEmpty()) {
      if (infoMsg.State.EngineVersion.IsEmpty()) {
        throw new Exception($"{pqStrForErr} is not a Community engine");
      }
      throw new Exception($"{pqStrForErr} has no gRPC connectivity available");
    }

    Uri uri;
    try {
      uri = new Uri(url);
    } catch (Exception e) {
      throw new Exception($"{pqStrForErr} has invalid url='{url}'", e);
    }

    var urlScheme = uri.Scheme;
    var urlTarget = uri.Host + uri.PathAndQuery;
    var envoyPrefix = infoMsg.State.ConnectionDetails.EnvoyPrefix;
    var scriptLanguage = infoMsg.Config.ScriptLanguage;
    var useTls = urlScheme == Uri.UriSchemeHttps;
    return ConnectToDndWorker(
      pqSerial,
      urlTarget,
      useTls,
      envoyPrefix,
      scriptLanguage);
  }

  private (Int64, Client) ConnectToDndWorker(
    Int64 pqSerial,
    string target,
    bool useTls,
    string envoyPrefix,
    string scriptLanguage) {
    var clientOptions = new ClientOptions();
    if (!envoyPrefix.IsEmpty()) {
      clientOptions.AddExtraHeader("envoy-prefix", envoyPrefix);
    }
    clientOptions.SetSessionType(scriptLanguage);

    if (useTls) {
      clientOptions.SetUseTls(true);
    }
    if (!_rootCerts.IsEmpty()) {
      clientOptions.SetTlsRootCerts(_rootCerts);
    }
    var authToken = _authClient.CreateToken(DispatcherServiceName);
    clientOptions.SetCustomAuthentication(
      "io.deephaven.proto.auth.Token",
      AuthUtil.AsBase64Proto(authToken));

    var client = Client.Connect(target, clientOptions);
    return (pqSerial, client);
  }


  public Subscription Subscribe() => _controllerClient.Subscribe();

  public void RemoveQuery(Int64 pqSerial) {
    _controllerClient.RemoveQuery(pqSerial);
  }

  private static string GetUrl(string url, bool validateCertificate) {
    var handler = new HttpClientHandler();
    if (!validateCertificate) {
      handler.ClientCertificateOptions = ClientCertificateOption.Manual;
      handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
    }
    using var hc = new HttpClient(handler);
    var result = hc.GetStringAsync(url).Result;
    return result;
  }
}
