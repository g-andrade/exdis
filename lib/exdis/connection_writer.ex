defmodule Exdis.ConnectionWriter do
  require Logger
  require Record

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  # @hibernate_after :timer.seconds(5)

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:state,
    conn_id: nil,
    conn_pid: nil,
    transport: nil,
    socket: nil
  )

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def start_link(conn_id, transport, socket) do
    init_args = [conn_id, self(), transport, socket]
    :proc_lib.start_link(__MODULE__, :init, init_args)
  end

  def dispatch(pid, type, args) do
    send(pid, {:write, type, args})
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Initialization
  ## ------------------------------------------------------------------

  def init(conn_id, conn_pid, transport, socket) do
    _ = Process.flag(:trap_exit, :true)
    :proc_lib.init_ack({:ok, self()})
    state = state(
      conn_id: conn_id,
      conn_pid: conn_pid,
      transport: transport,
      socket: socket)
    loop(state)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Loop
  ## ------------------------------------------------------------------

  defp loop(state) do
    #Logger.info("Writer awaiting message")
    receive do
      msg ->
        #Logger.info("Writer got message")
        handle_msg(msg, state)
    end
  end

  defp handle_msg({:write, type, args}, state) do
    handle_write(type, args, state)
  end

  defp handle_msg({:"EXIT", pid, _}, state(conn_pid: pid)) do
    exit(:normal)
  end

  def handle_write(:direct, resp_value, state) do
    _ = perform_direct_write(resp_value, state)
    loop(state)
  end

  def handle_write(:future, {key_owner_pid, future_ref}, state) do
    _ = perform_future_write(key_owner_pid, future_ref, state)
    loop(state)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Direct Write
  ## ------------------------------------------------------------------

  def perform_direct_write(resp_value, state) do
    #Logger.info("Direct write started")
    state(transport: transport, socket: socket) = state
    data = Exdis.RESP.Value.encode(resp_value)
    transport.send(socket, data)
    #Logger.info("Direct write finished")
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Future Write
  ## ------------------------------------------------------------------

  def perform_future_write(key_owner_pid, future_ref, state) do
    #Logger.info("Future write started")
    key_owner_mon = Process.monitor(key_owner_pid)
    perform_future_write_recur(key_owner_pid, key_owner_mon, future_ref, state)
  end

  def perform_future_write_recur(key_owner_pid, key_owner_mon, future_ref, state) do
    state(transport: transport, socket: socket) = state

    case Exdis.Database.KeyOwner.consume_future(key_owner_pid, key_owner_mon, future_ref) do
      {:more, _part} ->
        exit(:todo)
      {:finished, resp_value} ->
        data = Exdis.RESP.Value.encode(resp_value)
        _ = transport.send(socket, data)
        #Logger.info("Future write finished")
    end
  end
end
