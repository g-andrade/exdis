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

  def handle_write(:direct, value, state) do
    _ = perform_direct_write(value, state)
    loop(state)
  end

  def handle_write(:future, {key_owner_pid, future_ref}, state) do
    _ = perform_future_write(key_owner_pid, future_ref, state)
    loop(state)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Direct Write
  ## ------------------------------------------------------------------

  def perform_direct_write(value, state) do
    #Logger.info("Direct write started")
    state(transport: transport, socket: socket) = state
    data = 
      case value do 
        :ok -> 
          Exdis.RESP.Value.encode({:simple_string, "OK"})
        :queued -> 
          Exdis.RESP.Value.encode({:simple_string, "QUEUED"})
        {:error, reason} -> 
          reason_iodata = error_reason_to_iodata(reason)
          Exdis.RESP.Value.encode({:error, reason_iodata})
        other ->
          Exdis.RESP.Value.encode(other)
      end
    transport.send(socket, data)
    #Logger.info("Direct write finished")
  end

  defp error_reason_to_iodata(reason) do
    case reason do
      :bad_syntax ->
        "ERR syntax error"
      {:unknown_command, command_name} ->
        "ERR unknown command '#{command_name}'"
      :key_of_wrong_type ->
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      {:not_an_integer_or_out_of_range, argument_name} ->
        "ERR #{argument_name} is not an integer or out of range"
      :increment_or_decrement_would_overflow ->
        "ERR increment or decrement would overflow"
      {:not_a_valid_float, argument_name} ->
        "ERR #{argument_name} is not a valid float"
      :increment_would_produce_NaN_or_infinity ->
        "ERR increment would produce NaN or Infinity"
      {:calls_cannot_be_nested, command_name} ->
        "ERR #{command_name} calls can not be nested"
      {:command_without_another_first, dependent_name, dependency_name} ->
        "ERR #{dependent_name} without #{dependency_name}"
      :transaction_discarded_because_of_previous_errors ->
        "EXECABORT Transaction discarded because of previous errors"
    end
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
