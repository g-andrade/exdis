defmodule Exdis.Connection do
  require Logger
  require Record

  @behaviour :ranch_protocol

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  # @hibernate_after :timer.seconds(5)

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  ## ------------------------------------------------------------------
  ## :ranch_protocol Function Definitions
  ## ------------------------------------------------------------------

  def start_link(accept_ref, _listen_socket, transport, protocol_opts) do
    init_args = %{
      accept_ref: accept_ref,
      transport: transport,
      protocol_opts: protocol_opts
    }
    :proc_lib.start_link(__MODULE__, :init, [init_args])
  end

  def init(args) do
    :proc_lib.init_ack({:ok, self()})
    init_handshake(args)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Initialization
  ## ------------------------------------------------------------------

  defp init_handshake(init_args) do
    %{accept_ref: accept_ref} = init_args
    case :ranch.handshake(accept_ref) do
      {:ok, socket} ->
        init_connection_id(init_args, socket)
      {:error, _} ->
        exit(:normal)
    end
  end

  defp init_connection_id(init_args, socket) do
    %{transport: transport} = init_args
    case transport.peername(socket) do
      {:ok, remote_peer} ->
        conn_id = build_connection_id(init_args, remote_peer)
        in_buf = <<>>
        loop(conn_id, transport, socket, in_buf)
      {:error, _} ->
        exit(:normal)
    end
  end

  defp build_connection_id(init_args, remote_peer) do
    %{transport: transport} = init_args
    {remote_ip_address, remote_port} = remote_peer
    scheme = connection_id_scheme(transport)
    remote_ip_address_charlist = :inet.ntoa(remote_ip_address)
    remote_ip_address_str = List.to_string(remote_ip_address_charlist)
    "#{scheme}://#{remote_ip_address_str}:#{remote_port}"
  end

  defp connection_id_scheme(transport) do
    case transport do
      :ranch_tcp ->
        "redis"
      :ranch_ssl ->
        "rediss"
    end
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Loop
  ## ------------------------------------------------------------------

  defp loop(conn_id, transport, socket, in_buf) do
    command_parser = Exdis.Command.parser()
    receive_command_data(conn_id, transport, socket, in_buf, command_parser)
  end

  defp receive_command_data(conn_id, transport, socket, in_buf, parser) do
    case transport.recv(socket, 0, :infinity) do
      {:ok, data} ->
        in_buf = <<in_buf :: bytes, data :: bytes>>
        try_parsing_command(conn_id, transport, socket, in_buf, parser)
      {:error, _} ->
        exit(:normal)
    end
  end

  defp try_parsing_command(conn_id, transport, socket, in_buf, parser) do
    case parser.(in_buf) do
      {:parsed, command, in_buf} ->
        handle_command(conn_id, transport, socket, in_buf, command)
      {:more, parser, in_buf} ->
        receive_command_data(conn_id, transport, socket, in_buf, parser)
    end
  end

  defp handle_command(conn_id, transport, socket, in_buf, command) do
    case Exdis.Command.handle(command, &send_command_reply(&1, &2, transport, socket)) do
      :ok ->
        loop(conn_id, transport, socket, in_buf)
      {:error, reason} ->
        Logger.warn("#{conn_id} Failed to reply to command: #{inspect reason}")
        exit(:normal)
    end
  end

  defp send_command_reply(:sync, reply, transport, socket) do
    encoded_reply = Exdis.CommandReply.encode(reply)
    transport.send(socket, encoded_reply)
  end

  defp send_command_reply(:async, reply, :ranch_tcp, socket) do
    # TODO paginate very large values
    encoded_reply = Exdis.CommandReply.encode(reply)
    # XXX should we use `nosuspend` on the following in case of very large payloads?
    true = Port.command(socket, encoded_reply)
    monitor = Port.monitor(socket)
    continuation_cb = fn (status) -> {:finished, status} end
    {:await, socket, continuation_cb, monitor}
  end

  defp send_command_reply(:async, _reply, :ranch_ssl, _socket) do
    exit(:notsup)
  end

#  defp handle_command(conn_id, transport, socket, in_buf, command) do
#    {:go, regulator_ref, regulator_pid} = Exdis.Regulator.ask()
#    reply =
#      try do
#        Exdis.Command.handle(command)
#      after
#        Exdis.Regulator.done(regulator_pid, regulator_ref)
#      end
#    handle_command_reply(conn_id, transport, socket, in_buf, reply)
#  end
#
#  defp handle_command_reply(conn_id, transport, socket, in_buf, reply) do
#    case reply.() do
#      {:finished, reply_data} ->
#        write_to_socket(transport, socket, reply_data)
#        loop(conn_id, transport, socket, in_buf)
#      {:more, reply_data, reply} ->
#        write_to_socket(transport, socket, reply_data)
#        handle_command_reply(conn_id, transport, socket, in_buf, reply)
#    end
#  end
end
