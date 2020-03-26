defmodule Exdis.Database.KeyOwner do
  require Logger
  require Record

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @hibernate_after 5_000

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:state,
    global_committer: nil,
    active_lock: nil,
    lock_requests: :queue.new(),
    value: nil,
    value_streams: nil
  )

  Record.defrecord(:lock,
    epoch: nil,
    owner_pid: nil,
    owner_monitor: nil,
    ref: nil,
    streams_reader: nil,
    uncommited_value: nil
  )

  Record.defrecord(:lock_request,
    epoch: nil,
    owner_pid: nil,
    ref: nil,
    streams_reader: nil
  )

  Record.defrecord(:value_streams,
    all: %{},
    reader_monitors: %{}
  )

  Record.defrecord(:value_stream,
    reader_pid: nil,
    reader_monitor: nil,
    state: nil
  )

  ## ------------------------------------------------------------------
  ## API Functions
  ## ------------------------------------------------------------------

  def start_link_and_lock(lock_epoch, lock_owner_pid, lock_ref, streams_reader) do
    init_args = [self(), lock_epoch, lock_owner_pid, lock_ref, streams_reader]
    :proc_lib.start_link(__MODULE__, :init_and_lock, init_args)
  end

  def async_lock(pid, lock_epoch, lock_owner_pid, lock_ref, streams_reader) do
    send(pid, {:event, &handle_lock_request(&1, lock_epoch, lock_owner_pid, lock_ref, streams_reader)})
  end

  def read({pid, monitor}, handler) do
    call(pid, monitor, &handle_read_request(&1, &2, handler))
  end

  def write({pid, monitor}, handler) do
    call(pid, monitor, &handle_write_request(&1, &2, handler))
  end

  def consume_value_stream(pid, monitor, ref) do
    call(pid, monitor, &handle_value_stream_consumption_request(&1, &2, ref))
  end

  def release_locks(pids_and_monitors, ref) do
    [guiding_result | other_results] = multi_call(pids_and_monitors, &handle_lock_release_request(&1, &2, ref))
    case Enum.any?(other_results, fn result -> result != guiding_result end) do
      false ->
        {:ok, guiding_result}
      true ->
        pids = Enum.map(pids_and_monitors, fn {pid, _} -> pid end)
        exception_details = Enum.zip(pids, [guiding_result | other_results])
        exit({:mismatched_lock_release_results, ref, exception_details})
    end
  end

  ## ------------------------------------------------------------------
  ## OTP Process Functions
  ## ------------------------------------------------------------------

  # https://gist.github.com/marcelog/97708058cd17f86326c82970a7f81d40#file-simpleproc-erl

  def init_and_lock(parent, lock_epoch, lock_owner_pid, lock_ref, streams_reader) do
    Process.flag(:trap_exit, true)
    debug = :sys.debug_options([])
    global_committer = Exdis.GlobalCommitter.pid_and_monitor()
    lock_owner_monitor = Process.monitor(lock_owner_pid)
    Logger.debug("Newly locked by #{inspect lock_owner_pid} for epoch #{lock_epoch}")
    lock = lock(
      epoch: lock_epoch,
      owner_pid: lock_owner_pid,
      owner_monitor: lock_owner_monitor,
      ref: lock_ref,
      streams_reader: streams_reader)

    state = state(
      global_committer: global_committer,
      active_lock: lock,
      value_streams: value_streams())

    :proc_lib.init_ack(parent, {:ok, self()})
    loop(parent, debug, state)
  end

  def write_debug(dev, event, name) do
    :io.format(dev, '~p event = ~p~n', [name, event])
  end

  def system_continue(parent, debug, state) do
    loop(parent, debug, state)
  end

  def system_terminate(reason, _parent, _debug, _state) do
    exit(reason)
  end

  def system_code_change(state() = state, _module, _old_vsn, _extra) do
    {:ok, state}
  end

  ## ------------------------------------------------------------------
  ## Private Functions - Calls
  ## ------------------------------------------------------------------

  defp call(pid, monitor, handler) when is_reference(monitor) do
    reply_ref = monitor
    send_call(pid, reply_ref, handler)
    receive_call_reply(monitor, reply_ref)
  end

  defp call(pid, nil, handler) do
    monitor = Process.monitor(pid)
    try do
      call(pid, monitor, handler)
    after
      Process.demonitor(monitor, [:flush])
    end
  end

  defp send_call(pid, reply_ref, handler) do
    send(pid, {:call, self(), reply_ref, handler})
  end

  defp receive_call_reply(monitor, reply_ref) do
    receive do
      {^reply_ref, reply} ->
        case reply do
          {:exception, reason} ->
            :erlang.error(reason)
          reply ->
            reply
        end
      {:"DOWN", ^reply_ref, _, _, reason} ->
        exit({reason, {__MODULE__, :call, [monitor, reply_ref]}})
    end
  end

  defp multi_call(pids_and_monitors, handler) do
    multi_call_send_recur(pids_and_monitors, handler, [])
  end

  defp multi_call_send_recur([{pid, monitor} | next], handler, monitors_of_notified_pids_acc) do
    reply_ref = monitor
    send_call(pid, reply_ref, handler)
    monitors_of_notified_pids_acc = [monitor | monitors_of_notified_pids_acc]
    multi_call_send_recur(next, handler, monitors_of_notified_pids_acc)
  end

  defp multi_call_send_recur([], _handler, monitors_of_notified_pids) do
    monitors_of_notified_pids = Enum.reverse(monitors_of_notified_pids)
    multi_call_receive_replies_recur(monitors_of_notified_pids, [])
  end

  defp multi_call_receive_replies_recur([monitor | next], replies_acc) do
    reply_ref = monitor
    reply = receive_call_reply(monitor, reply_ref)
    replies_acc = [reply | replies_acc]
    multi_call_receive_replies_recur(next, replies_acc)
  end

  defp multi_call_receive_replies_recur([], replies_acc) do
    Enum.reverse(replies_acc)
  end

  ## ------------------------------------------------------------------
  ## Private Functions - Loop
  ## ------------------------------------------------------------------

  defp loop(parent, debug, state) do
    receive do
      msg ->
        handle_msg_and_loop(parent, debug, state, msg)
    after
      @hibernate_after ->
        :proc_lib.hibernate(__MODULE__, :system_continue, [parent, debug, state])
    end
  end

  defp handle_msg_and_loop(parent, debug, state, msg) do
    case msg do
      {:call, caller_pid, reply_ref, handler} ->
        {reply, state} = handler.(state, caller_pid)
        send(caller_pid, {reply_ref, reply})
        loop(parent, debug, state)
      {:event, handler} ->
        state = handler.(state)
        loop(parent, debug, state)
      {:"DOWN", ref, :process, _, _} ->
        state = handle_monitored_process_death(state, ref)
        loop(parent, debug, state)
      {:"EXIT", ^parent, reason} ->
        exit(reason)
      {:system, from, request} ->
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, state)
    end
  end

  defp handle_lock_request(state, epoch, owner_pid, ref, streams_reader) do
    case state do
      state(active_lock: nil, value: value) ->
        Logger.debug("Newly locked by #{inspect owner_pid} for epoch #{epoch}")
        owner_monitor = Process.monitor(owner_pid)
        lock = lock(
          epoch: epoch,
          owner_pid: owner_pid,
          owner_monitor: owner_monitor,
          ref: ref,
          streams_reader: streams_reader,
          uncommited_value: value
        )
        state(state, active_lock: lock)

      state(lock_requests: queue) ->
        Logger.debug("Enqueued lock request by #{inspect owner_pid} for epoch #{epoch}")
        lock_request = lock_request(
          epoch: epoch,
          owner_pid: owner_pid,
          ref: ref,
          streams_reader: streams_reader
        )
        queue = :queue.in(lock_request, queue)
        state(state, lock_requests: queue)
    end
  end

  defp handle_next_lock_request(state) do
    state(lock_requests: queue, value: value) = state
    case :queue.out(queue) do
      {{:value, lock_request}, queue} ->
        lock = lock_request_to_lock(lock_request, value)
        lock(epoch: epoch, owner_pid: owner_pid) = lock
        Logger.debug("Newly locked by #{inspect owner_pid} for epoch #{epoch}")
        state(state, active_lock: lock, lock_requests: queue)
      {:empty, _} ->
        state
    end
  end

  defp lock_request_to_lock(lock_request, value) do
    lock_request(
      epoch: epoch,
      owner_pid: owner_pid,
      ref: ref,
      streams_reader: streams_reader) = lock_request

    owner_monitor = Process.monitor(owner_pid)
    lock(
      epoch: epoch,
      owner_pid: owner_pid,
      owner_monitor: owner_monitor,
      ref: ref,
      streams_reader: streams_reader,
      uncommited_value: value)
  end

  defp handle_monitored_process_death(state, ref) do
    case state do
      state(active_lock: lock(epoch: epoch, owner_monitor: ^ref, owner_pid: owner_pid)) ->
        Logger.warn("Active lock by #{inspect owner_pid} for epoch #{epoch} discarded after its owner stopped")
        state = state(state, active_lock: nil)
        handle_next_lock_request(state)

      state(value_streams: value_streams(reader_monitors: %{^ref => stream_ref}) = streams) ->
        value_streams(all: all, reader_monitors: reader_monitors) = streams
        {value_stream(reader_pid: reader_pid), all} = Map.pop(all, stream_ref)
        reader_monitors = Map.delete(reader_monitors, ref)
        streams = value_streams(streams, all: all, reader_monitors: reader_monitors)
        Logger.warn("Value stream #{inspect stream_ref} deleted after its reader #{inspect reader_pid} stopped")
        state(state, value_streams: streams)
    end
  end

  ## ------------------------------------------------------------------
  ## Private Functions
  ## ------------------------------------------------------------------

  defp handle_read_request(state, from_pid, handler) do
    state(active_lock: lock) = state

    case lock do
      lock(owner_pid: ^from_pid, streams_reader: streams_reader, uncommited_value: value) ->
        case handler.(value) do
          {:ok, reply_success} ->
            maybe_prepare_streamed_reply_success(state, reply_success, streams_reader)
          {:ok, reply_success, value} ->
            state = update_uncommitted_value(state, lock, value)
            maybe_prepare_streamed_reply_success(state, reply_success, streams_reader)
          {:error, _} = reply ->
            {reply, state}
          {:error, reason, value} ->
            state = update_uncommitted_value(state, lock, value)
            {{:error, reason}, state}
        end

      lock(owner_pid: owner_pid) ->
        reply = {:exception, {{:lock_owned_by, owner_pid}, {__MODULE__, :handle_read_request}}}
        {reply, state}
      nil ->
        reply = {:exception, {:not_locked, {__MODULE__, :handle_read_request}}}
        {reply, state}
    end
  end

  defp handle_write_request(state, from_pid, handler) do
    state(active_lock: lock) = state

    case lock do
      lock(owner_pid: ^from_pid, streams_reader: streams_reader, uncommited_value: value) ->
        case handler.(value) do
          {:ok, reply_success, value} ->
            state = update_uncommitted_value(state, lock, value)
            maybe_prepare_streamed_reply_success(state, reply_success, streams_reader)
          {:error, _} = reply ->
            {reply, state}
          {:error, reason, value} ->
            state = update_uncommitted_value(state, lock, value)
            {{:error, reason}, state}
        end

      lock(owner_pid: owner_pid) ->
        reply = {:exception, {{:lock_owned_by, owner_pid}, {__MODULE__, :handle_write_request}}}
        {reply, state}
      nil ->
        reply = {:exception, {:not_locked, {__MODULE__, :handle_write_request}}}
        {reply, state}
    end
  end

  defp handle_lock_release_request(state, from_pid, ref) do
    state(global_committer: global_committer, active_lock: lock) = state

    case lock do
      lock(ref: ^ref, owner_pid: ^from_pid, epoch: epoch) ->
        lock(owner_monitor: owner_monitor, uncommited_value: value) = lock
        Process.demonitor(owner_monitor, [:flush])
        case Exdis.GlobalCommitter.release_key_lock(global_committer, epoch, ref, self()) do
          :committed ->
            Logger.info("Committed value: #{inspect value}")
            state = state(state, active_lock: nil, value: value)
            state = handle_next_lock_request(state)
            {:committed, state}
          :discarded ->
            state = handle_next_lock_request(state)
            {:discarded, state}
        end

      lock(ref: ^ref, owner_pid: owner_pid) ->
        reply = {:exception, {{:lock_owned_by, owner_pid}, {__MODULE__, :handle_lock_release_request}}}
        {reply, state}
      lock(ref: ref) ->
        reply = {:exception, {{:wrong_lock_ref, ref}, {__MODULE__, :handle_lock_release_request}}}
        {reply, state}
      nil ->
        reply = {:exception, {:not_locked, {__MODULE__, :handle_lock_release_request}}}
        {reply, state}
    end
  end

  defp update_uncommitted_value(state, lock, value) do
    lock = lock(lock, uncommited_value: value)
    state(state, active_lock: lock)
  end

  defp maybe_prepare_streamed_reply_success(state, {:stream, stream_state}, reader) do
    state(value_streams: streams) = state
    value_streams(all: all, reader_monitors: reader_monitors) = streams
    ref = make_ref()
    reader_pid = stream_reader_pid(state, reader)
    reader_monitor = Process.monitor(reader_pid)
    value_stream = value_stream(
      reader_pid: reader_pid,
      reader_monitor: reader_monitor,
      state: stream_state)

    reply = {:ok, {:stream, self(), ref}}
    all = Map.put(all, ref, value_stream)
    reader_monitors = Map.put(reader_monitors, reader_monitor, ref)
    streams = value_streams(streams, all: all, reader_monitors: reader_monitors)
    state = state(state, value_streams: streams)
    {reply, state}
  end

  defp maybe_prepare_streamed_reply_success(state, reply_success, _) do
    case reply_success do
      :ok ->
        {:ok, state}
      _ ->
        reply = {:ok, reply_success}
        {reply, state}
    end
  end

  defp stream_reader_pid(state, reader) do
    cond do
      reader === :lock_owner ->
        state(active_lock: lock(owner_pid: pid)) = state
        pid
      is_pid(reader) ->
        reader
    end
  end

  defp handle_value_stream_consumption_request(state, from_pid, ref) do
    state(value_streams: streams) = state
    value_streams(all: all) = streams

    case Map.get(all, ref) do
      value_stream(reader_pid: ^from_pid, state: stream_state) = stream ->
        case Exdis.Database.Value.Stream.consume(stream_state) do
          {:more, part, stream_state} ->
            reply = {:more, part}
            value_stream = value_stream(stream, state: stream_state)
            all = Map.replace!(all, ref, value_stream)
            streams = value_streams(streams, all: all)
            state = state(state, value_streams: streams)
            {reply, state}
          {:finished, _} = reply ->
            value_streams(all: all, reader_monitors: reader_monitors) = streams
            value_stream(reader_monitor: reader_monitor) = stream
            Process.demonitor(reader_monitor, [:flush])
            all = Map.delete(all, ref)
            {^ref, reader_monitors} = Map.pop(reader_monitors, reader_monitor)
            streams = value_streams(streams, all: all, reader_monitors: reader_monitors)
            state = state(state, value_streams: streams)
            {reply, state}
        end

      value_stream() ->
        reply = {:exception, {:not_the_stream_owner, {__MODULE__, :handle_value_stream_consumption_request}}}
        {reply, state}
      nil ->
        reply = {:exception, {:stream_not_found, {__MODULE__, :handle_value_stream_consumption_request}}}
        {reply, state}
    end
  end
end
