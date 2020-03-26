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

  def dispatch(pid, writes) do
    send(pid, {:write, writes})
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
        handle_msg(state, msg)
    end
  end

  defp handle_msg(state, {:write, writes}) do
    handle_writes(state, writes)
  end

  defp handle_msg(state(conn_pid: pid), {:"EXIT", pid, _}) do
    exit(:normal)
  end

  defp handle_writes(state, [{:stream, key_owner_pid, stream_ref} | next]) do
    _ = perform_streamed_write(state, key_owner_pid, stream_ref)
    handle_writes(state, next)
  end

  defp handle_writes(state, [resp_value | next]) do
    _ = perform_direct_write(state, resp_value)
    handle_writes(state, next)
  end

  defp handle_writes(state, []) do
    loop(state)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Direct Write
  ## ------------------------------------------------------------------

  def perform_direct_write(state, value) do
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
    Logger.info("Writing #{inspect data}")
    transport.send(socket, data)
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

  def perform_streamed_write(state, key_owner_pid, stream_ref) do
    key_owner_mon = Process.monitor(key_owner_pid)
    perform_future_write_recur(state, key_owner_pid, key_owner_mon, stream_ref)
  end

  def perform_future_write_recur(state, key_owner_pid, key_owner_mon, stream_ref) do
    case Exdis.Database.KeyOwner.consume_value_stream(key_owner_pid, key_owner_mon, stream_ref) do
      {:more, value} ->
        perform_direct_write(state, value)
        perform_future_write_recur(state, key_owner_pid, key_owner_mon, stream_ref)
      {:finished, value} ->
        perform_direct_write(state, value)
        Process.demonitor(key_owner_mon, [:flush])
    end
  end
end
