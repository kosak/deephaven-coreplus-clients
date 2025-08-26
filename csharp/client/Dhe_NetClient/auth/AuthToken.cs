//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Google.Protobuf;

namespace Deephaven.Dhe_NetClient;

public record AuthToken(
  UInt64 TokenId,
  string Service,
  UserContext UserContext,
  // This is either the raw 4 bytes representing an ipv4 address or
  // the raw 16 bytes representing an ipv6 address.
  ByteString OriginIpAddressBytes);
