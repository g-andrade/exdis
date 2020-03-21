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
    case Exdis.Command.handle(command) do
      :ok ->
        send_whole_command_reply(transport, socket, :ok)
        loop(conn_id, transport, socket, in_buf)

      {:error, _} = error ->
        send_whole_command_reply(transport, socket, error)
        loop(conn_id, transport, socket, in_buf)

      {:async_ok, key_owner_pid, key_owner_mon, future_ref} ->
        handle_async_command_reply(
          conn_id, transport, socket, in_buf,
          key_owner_pid, key_owner_mon, future_ref)
    end
  end

  defp handle_async_command_reply(
    conn_id, transport, socket, in_buf,
    key_owner_pid, key_owner_mon, future_ref)
  do
    #
    # FIXME redo this mess of a loop
    #
    case Exdis.Database.KeyOwner.consume_future(key_owner_pid, key_owner_mon, future_ref) do
      {:more, chunk} ->
        send_partial_command_reply(transport, socket, chunk)
        handle_async_command_reply(
          conn_id, transport, socket, in_buf,
          key_owner_pid, key_owner_mon, future_ref)

      {:finished, chunk} ->
        send_partial_command_reply(transport, socket, chunk)
        loop(conn_id, transport, socket, in_buf)
    end
  end

  defp send_whole_command_reply(transport, socket, reply) do
    data = Exdis.CommandReply.encode(reply)
    write_to_socket(transport, socket, data)
  end

  defp send_partial_command_reply(transport, socket, chunk) do
    # TODO
    send_whole_command_reply(transport, socket, chunk)
  end

  defp write_to_socket(transport, socket, data) do
    case transport.send(socket, data) do
      :ok ->
        :ok
      {:error, reason} ->
        Logger.warn("Failed to write to socket: #{inspect reason}")
        exit(:normal)
    end
  end
end
