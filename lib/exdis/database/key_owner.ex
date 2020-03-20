defmodule Exdis.Database.KeyOwner do
  use GenServer
  require Record

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:state,
    registry_pid: nil,
    value_state: nil,
    ongoing_replies: %{},
    ongoing_reply_monitors: %{}
  )

  Record.defrecord(:ongoing_reply,
    continuation_cb: nil,
    monitor: nil,
    call_finish_cb: nil
  )

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def read(key, read_cb, default, reply_cb, retries_left \\ 2) do
    case Exdis.Database.KeyRegistry.get_owner(key) do
      nil ->
        reply_cb.(:sync, default)
      pid ->
        try do
          GenServer.call(pid, {:read, read_cb, reply_cb}, :infinity)
        catch
          :exit, {reason, {GenServer, :call, [^pid|_]}}
          when reason in [:noproc, :normal] and retries_left > 0 ->
            read(key, read_cb, default, retries_left - 1)
        end
    end
  end

  def upsert(key, update_cb, init_cb, reply_cb, retries_left \\ 2) do
    # FIXME the code below is a mess
    case Exdis.Database.KeyRegistry.get_owner(key) do
      nil ->
        case start(key, init_cb, reply_cb) do
          {:registered, retval} ->
            retval
          {:error, {:already_registered, _}} ->
            upsert(key, update_cb, init_cb, reply_cb, retries_left - 1)
        end
      pid ->
        try do
          GenServer.call(pid, {:update, update_cb, reply_cb}, :infinity)
        catch
          :exit, {reason, {GenServer, :call, [^pid|_]}}
          when reason in [:noproc, :normal] and retries_left > 0 ->
            upsert(key, update_cb, init_cb, reply_cb, retries_left - 1)
        end
    end
  end

  ## ------------------------------------------------------------------
  ## proc_lib Function Definitions
  ## ------------------------------------------------------------------

  def proc_lib_init(key, init_cb, reply_cb) do
    _ = Process.flag(:trap_exit, true)
    case Exdis.Database.KeyRegistry.register_owner(key) do
      {:ok, registry_pid} ->
        {reply, value_state} = init_cb.()
        call_finish_cb = fn (retval) -> :proc_lib.init_ack({:registered, retval}) end
        state = state(registry_pid: registry_pid, value_state: value_state)
        state = begin_async_reply(reply, reply_cb, call_finish_cb, state)
        :gen_server.enter_loop(__MODULE__, [], state)
      {:error, {:already_registered, existing_pid}} ->
        :proc_lib.init_ack({:error, {:already_registered, existing_pid}})
    end
  end

  ## ------------------------------------------------------------------
  ## GenServer Function Definitions
  ## ------------------------------------------------------------------

  def init(_) do
    exit(:not_supposed_to_run)
  end

  def handle_call({:read, read_cb, reply_cb}, from, state) do
    handle_read_call(read_cb, reply_cb, from, state)
  end

  def handle_call({:update, update_cb, reply_cb}, from, state) do
    handle_update_call(update_cb, reply_cb, from, state)
  end

  def handle_call(call, from, state) do
    {:stop, {:unexpected_call, from, call}, state}
  end

  def handle_cast(cast, state) do
    {:stop, {:unexpected_cast, cast}, state}
  end

  def handle_info({:inet_reply, tag, status}, state) do
    # gen_tcp async reply
    handle_async_reply_status(tag, status, state)
  end

  def handle_info({ref, value}, state) when is_reference(ref) do
    state(ongoing_replies: ongoing_replies) = state
    case Map.has_key?(ongoing_replies, ref) do
      true ->
        # ssl async reply
        handle_async_reply_status(ref, value, state)
      false ->
        {:stop, {:unexpected_info, {ref, value}}, state}
    end
  end

  def handle_info({:"DOWN", ref, _, _, reason} = info, state) do
    state(ongoing_reply_monitors: ongoing_reply_monitors) = state
    case Map.has_key?(ongoing_reply_monitors, ref) do
      true ->
        # socket error
        handle_async_reply_death(ref, reason, state)
      false ->
        {:stop, {:unexpected_info, info}, state}
    end
  end

  def handle_info({:"EXIT", pid, _}, state(registry_pid: pid) = state) do
    {:stop, :normal, state}
  end

  def handle_info(info, state) do
    {:stop, {:unexpected_info, info}, state}
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  def start(key, init_cb, reply_cb) do
    :proc_lib.start(__MODULE__, :proc_lib_init, [key, init_cb, reply_cb])
  end

  def handle_read_call(read_cb, reply_cb, from, state) do
    state(value_state: value_state) = state
    case read_cb.(value_state) do
      {reply} ->
        call_finish_cb = fn (retval) -> GenServer.reply(from, retval) end
        state = begin_async_reply(reply, reply_cb, call_finish_cb, state)
        {:noreply, state}
      {reply, value_state} ->
        call_finish_cb = fn (retval) -> GenServer.reply(from, retval) end
        state = state(state, value_state: value_state)
        state = begin_async_reply(reply, reply_cb, call_finish_cb, state)
        {:noreply, state}
    end
  end

  def handle_update_call(update_cb, reply_cb, from, state) do
    state(value_state: value_state) = state
    {reply, value_state} = update_cb.(value_state)
    call_finish_cb = fn (retval) -> GenServer.reply(from, retval) end
    state = state(state, value_state: value_state)
    state = begin_async_reply(reply, reply_cb, call_finish_cb, state)
    {:noreply, state}
  end

  def begin_async_reply(reply, reply_cb, call_finish_cb, state) do
    state(ongoing_replies: replies, ongoing_reply_monitors: monitors) = state
    {:await, tag, continuation_cb, monitor} = reply_cb.(:async, reply)
    false = Map.has_key?(replies, tag)
    false = Map.has_key?(monitors, monitor)
    reply_state =
      ongoing_reply(
        continuation_cb: continuation_cb,
        monitor: monitor,
        call_finish_cb: call_finish_cb)

    replies = Map.put(replies, tag, reply_state)
    monitors = Map.put(monitors, monitor, tag)
    state(state, ongoing_replies: replies, ongoing_reply_monitors: monitors)
  end

  def handle_async_reply_status(tag, status, state) do
    state(ongoing_replies: replies, ongoing_reply_monitors: monitors) = state
    reply_state = Map.fetch!(replies, tag)
    ongoing_reply(
      continuation_cb: continuation_cb, monitor: monitor,
      call_finish_cb: call_finish_cb) = reply_state

    case continuation_cb.(status) do
      {:finished, retval} ->
        :erlang.demonitor(monitor, [:flush])
        replies = Map.delete(replies, tag)
        monitors = Map.delete(monitors, monitor)
        _ = call_finish_cb.(retval)
        state = state(state, ongoing_replies: replies, ongoing_reply_monitors: monitors)
        {:noreply, state}

      {:sending, continuation_cb} ->
        reply_state = ongoing_reply(reply_state, continuation_cb: continuation_cb)
        replies = Map.replace!(replies, tag, reply_state)
        state = state(state, ongoing_replies: replies, ongoing_reply_monitors: monitors)
        {:noreply, state}
    end
  end

  def handle_async_reply_death(monitor, reason, state) do
    state(ongoing_replies: replies, ongoing_reply_monitors: monitors) = state
    {tag, monitors} = Map.fetch!(monitors, monitor)
    {reply_state, replies} = Map.fetch!(replies, tag)

    ongoing_reply(call_finish_cb: call_finish_cb) = reply_state
    _ = call_finish_cb.({:error, {:stopped, reason}})
    state = state(ongoing_replies: replies, ongoing_reply_monitors: monitors)
    {:noreply, state}
  end
end
