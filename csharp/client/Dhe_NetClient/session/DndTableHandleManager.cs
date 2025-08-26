//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;
using Google.Protobuf;
using Io.Deephaven.Proto.Backplane.Grpc;

namespace Deephaven.Dhe_NetClient;

public class DndTableHandleManager : TableHandleManager {
  internal DndTableHandleManager(Ticket? consoleId, Server server) : base(consoleId, server) {
  }

  /// <summary>
  /// Fetches a historical table from the database on the server.
  /// </summary>
  /// <param name="tableNamespace">The namespace of the table to fetch</param>
  /// <param name="tableName">the name of the table to fetch</param>
  /// <returns>the TableHandle of the fetched table.</returns>
  public TableHandle HistoricalTable(string tableNamespace, string tableName) {
    return MakeTableHandleFromTicketText($"d/hist/{tableNamespace}/{tableName}");
  }

  /// <summary>
  /// Fetches a live table from the database on the server.
  /// </summary>
  /// <param name="tableNamespace">The namespace of the table to fetch</param>
  /// <param name="tableName">the name of the table to fetch</param>
  /// <returns>the TableHandle of the fetched table.</returns>
  public TableHandle LiveTable(string tableNamespace, string tableName) {
    return MakeTableHandleFromTicketText($"d/live/{tableNamespace}/{tableName}");
  }

  /// <summary>
  /// Fetches the catalog table from the database on the server.
  /// </summary>
  /// <returns>the TableHandle of the catalog table.</returns>
  public TableHandle CatalogTable() {
    return MakeTableHandleFromTicketText("d/catalog");
  }

  private TableHandle MakeTableHandleFromTicketText(string ticketText) {
    var ticket = new Ticket {
      Ticket_ = ByteString.CopyFromUtf8(ticketText)
    };
    return MakeTableHandleFromTicket(ticket);
  }
}
