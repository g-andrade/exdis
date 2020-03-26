defmodule Exdis.Database do
  require Logger
  require Record

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @server __MODULE__

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:state,
    key_owners: %{},
    pids_to_keys: %{},
    global_committer_pid: nil,
    lock_epoch: nil
  )

  ## ------------------------------------------------------------------
  ## API Functions
  ## ------------------------------------------------------------------

  def child_spec([]) do
    %{
      id: @server,
      start: {__MODULE__, :start_link, []}
    }
  end

  def start_link() do
    :proc_lib.start_link(__MODULE__, :init, [self()])
  end

  def pid_and_monitor() do
    pid = Process.whereis(@server)
    monitor = Process.monitor(pid)
    {:ok, pid, monitor}
  end

  def async_lock_keys(pid, monitor, ref, keys, streams_reader \\ :lock_owner) do
    reverse_keys = Enum.reverse(keys)
    call(pid, monitor, &dispatch_key_lock_requests(&1, &2, ref, streams_reader, reverse_keys))
  end

  ## ------------------------------------------------------------------
  ## OTP Process Functions
  ## ------------------------------------------------------------------

  # https://gist.github.com/marcelog/97708058cd17f86326c82970a7f81d40#file-simpleproc-erl

  def init(parent) do
    _ = Process.flag(:trap_exit, true)
    debug = :sys.debug_options([])
    Process.register(self(), @server)

    {:ok, global_committer_pid, committed_epoch} = Exdis.GlobalCommitter.start_link()
    state = state(global_committer_pid: global_committer_pid, lock_epoch: committed_epoch + 1)
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
  ## Private Functions
  ## ------------------------------------------------------------------

  defp call(pid, monitor, handler) do
    reply_ref = monitor
    send(pid, {:call, self(), reply_ref, handler})
    receive do
      {^reply_ref, reply} ->
        reply
      {:"DOWN", ^reply_ref, _, _, reason} ->
        exit({reason, {__MODULE__, :call, [pid, monitor, handler]}})
    end
  end

  defp loop(parent, debug, state) do
    receive do
      msg ->
        handle_msg(parent, debug, state, msg)
    end
  end

  defp handle_msg(parent, debug, state, msg) do
    case msg do
      {:call, reply_pid, reply_ref, handler} ->
        {reply, state} = handler.(state, reply_pid)
        send(reply_pid, {reply_ref, reply})
        loop(parent, debug, state)
      {:system, from, request} ->
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, state)
      {:"EXIT", pid, reason} ->
        state = deal_with_linked_process_death(parent, state, pid, reason)
        loop(parent, debug, state)
    end
  end

  ## ------------------------------------------------------------------
  ## Private Functions - Lock Keys
  ## ------------------------------------------------------------------

  defp dispatch_key_lock_requests(state, pid, ref, streams_reader, keys) do
    state(key_owners: key_owners, lock_epoch: epoch) = state
    dispatch_key_lock_requests_recur(state, key_owners, epoch, pid, ref, streams_reader, keys, [])
  end

  defp dispatch_key_lock_requests_recur(
    state, key_owners, epoch, pid, ref, streams_reader, [key | next_keys], reverse_notified_pids)
  do
    case Map.get(key_owners, key) do
      key_owner_pid when is_pid(key_owner_pid) ->
        Exdis.Database.KeyOwner.async_lock(key_owner_pid, epoch, pid, ref, streams_reader)
        reverse_notified_pids = [key_owner_pid | reverse_notified_pids]
        dispatch_key_lock_requests_recur(
          state, key_owners, epoch, pid, ref, streams_reader, next_keys, reverse_notified_pids)
      nil ->
        state(pids_to_keys: pids_to_keys) = state
        {:ok, key_owner_pid} = Exdis.Database.KeyOwner.start_link_and_lock(epoch, pid, ref, streams_reader)
        key_owners = Map.put(key_owners, key, key_owner_pid)
        pids_to_keys = Map.put(pids_to_keys, key_owner_pid, key)
        state = state(state, key_owners: key_owners, pids_to_keys: pids_to_keys)
        reverse_notified_pids = [key_owner_pid | reverse_notified_pids]
        dispatch_key_lock_requests_recur(
          state, key_owners, epoch, pid, ref, streams_reader, next_keys, reverse_notified_pids)
    end
  end

  defp dispatch_key_lock_requests_recur(
    state, _key_owners, epoch, _pid, ref, _streams_reader, [], reverse_notified_pids)
  do
    state(global_committer_pid: global_committer_pid) = state
    Exdis.GlobalCommitter.notify_of_new_key_locks(global_committer_pid, epoch, ref, reverse_notified_pids)
    reply = reverse_notified_pids # to be reversed on caller
    state = state(state, lock_epoch: epoch + 1)
    {reply, state}
  end

  ## ------------------------------------------------------------------
  ## Private Functions - Death of Linked Processes
  ## ------------------------------------------------------------------

  defp deal_with_linked_process_death(parent, _state, pid, reason) when pid === parent do
    exit(reason)
  end

  defp deal_with_linked_process_death(_parent, state, pid, reason) do
    case state do
      state(pids_to_keys: %{^pid => key} = pids_to_keys, key_owners: key_owners) ->
        Logger.warn("Owner #{inspect pid} of key #{inspect key} has stopped unexpectedly")
        state(global_committer_pid: global_committer_pid) = state
        Exdis.GlobalCommitter.notify_of_key_owner_death(global_committer_pid, pid)
        pids_to_keys = Map.delete(pids_to_keys, pid)
        key_owners = Map.delete(key_owners, key)
        state(state, key_owners: key_owners, pids_to_keys: pids_to_keys)

      state(global_committer_pid: ^pid) ->
        exit({:global_committer_stopped, %{pid: pid, reason: reason}})
    end
  end
end
