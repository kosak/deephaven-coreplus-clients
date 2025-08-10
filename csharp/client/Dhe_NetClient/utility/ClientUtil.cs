//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Google.Protobuf;
using Io.Deephaven.Proto.Auth;

namespace Deephaven.Dhe_NetClient;

public static class ClientUtil {
  public static string GetName(string descriptiveName) {
    return DhCoreTodo.GetHostname() + "/" + descriptiveName;
  }

  public static ClientId MakeClientId(string descriptiveName, byte[] uuid) {
    var name = GetName(descriptiveName);
    var clientId = new ClientId {
      Name = name,
      Uuid = ByteString.CopyFrom(uuid)
    };
    return clientId;
  }
}

public static class DhCoreTodo {
  public static string GetHostname() {
    return "TODO-hostname";
  }
}
