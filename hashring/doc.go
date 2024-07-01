// Package balancer implements a gRPC Balancer backed by a consistent
// hashring for connection selection.
//
// As new connections are established new virtual nodes (members) are
// added to the hashring. When a connection is not in a Ready state we
// remove the member from the hashring.
package hashring
