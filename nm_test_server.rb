require_relative "node_listener"

server = MessagePack::RPC::Server.new
server.listen('127.0.0.1', 9090, NodeMonitorHandler.new)
server.run
