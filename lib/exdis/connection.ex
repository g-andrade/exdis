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

  Record.defrecord(:state,
    id: nil,
    writer_pid: nil,
    transport: nil,
    socket: nil,
    command_handler: nil
  )

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
        {:ok, writer_pid} = Exdis.ConnectionWriter.start_link(conn_id, transport, socket)
        command_handler = Exdis.CommandHandler.new(writer_pid)
        state = state(
          id: conn_id,
          writer_pid: writer_pid,
          transport: transport,
          socket: socket,
          command_handler: command_handler)
        loop(state)
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

  defp loop(state) do
    state = receive_and_handle_command(state)
    loop(state)
  end

  defp receive_and_handle_command(state) do
    state(transport: transport, socket: socket, command_handler: command_handler) = state
    try do
      command_handler = Exdis.CommandHandler.receive_and_handle(command_handler, &receive_from_socket(transport, socket, &1))
      state(state, command_handler: command_handler)
    catch
      :socket_closed ->
        exit(:normal)
    end
  end

  defp receive_from_socket(transport, socket, :line) do
    _ = transport.setopts(socket, [packet: :line])
    case transport.recv(socket, 0, :infinity) do
      {:ok, data} ->
        _ = transport.setopts(socket, [packet: :raw])
        data
      {:error, :closed} ->
        throw(:socket_closed)
    end
  end

  defp receive_from_socket(transport, socket, length) do
    case transport.recv(socket, length, :infinity) do
      {:ok, data} ->
        data
      {:error, :closed} ->
        throw(:socket_closed)
    end
  end
end
