//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Diagnostics;
using Deephaven.Dh_NetClient;

namespace Deephaven.Dhe_NetClient;

public class DndClient : Client {
  public static DndClient Create(Int64 pqSerial, SessionManager sessionManager,
    Client client, bool removeOnClose) {
    var thm = client.ReleaseTableHandleManager();
    var (consoleId, server) = thm.ReleaseServer();
    var dndThm = new DndTableHandleManager(consoleId, server);
    return new DndClient(pqSerial, sessionManager, dndThm, removeOnClose);
  }

  public readonly Int64 PqSerial;
  private readonly SessionManager _sessionManager;
  private readonly bool _removeOnClose;
  private bool _isDisposed = false;

  private DndClient(Int64 pqSerial, SessionManager sessionManager,
    DndTableHandleManager tableHandleManager, bool removeOnClose)
    : base(tableHandleManager) {
    PqSerial = pqSerial;
    _sessionManager = sessionManager;
    _removeOnClose = removeOnClose;
  }

  protected override void Dispose(bool disposing) {
    if (!_isDisposed) {
      _isDisposed = true;

      if (_removeOnClose) {
        try {
          _sessionManager.RemoveQuery(PqSerial);
        } catch (Exception e) {
          Debug.WriteLine($"Ignored exception: {e}");
        }
      }
    }

    base.Dispose(disposing);
  }


  /// <summary>
  /// Gets the TableHandleManager which you can use to create empty tables, fetch tables,
  /// and so on.
  /// </summary>
  public new DndTableHandleManager Manager => (DndTableHandleManager)base.Manager;
}
