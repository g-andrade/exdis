defmodule Exdis.GlobalCommitter do
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
    committed_epoch: nil,
    queue: %{}
  )

  Record.defrecord(:in_progress,
    lock_ref: nil,
    locked: nil,
    released: [],
    commit: true
  )

  ## ------------------------------------------------------------------
  ## API Functions
  ## ------------------------------------------------------------------

  def start_link() do
    :proc_lib.start_link(__MODULE__, :init, [self()])
  end

  def pid_and_monitor() do
    pid = Process.whereis(@server)
    monitor = Process.monitor(pid)
    {pid, monitor}
  end

  def notify_of_new_key_locks(pid, epoch, lock_ref, key_owner_pids) when key_owner_pids !== [] do
    send(pid, {:keys_locked, epoch, lock_ref, key_owner_pids})
  end

  def release_key_lock({pid, monitor}, epoch, lock_ref, key_owner_pid, key_owner_commitment \\ :commit)
  when key_owner_commitment in [:commit, :discard] do
    send(pid, {:release_key_lock, epoch, key_owner_pid, key_owner_commitment})
    receive do
      {^lock_ref, reply} ->
        reply
      {:"DOWN", ^monitor, _, _, reason} ->
        exit({reason, {__MODULE__, :release_key_lock}})
    end
  end

  def notify_of_key_owner_death(pid, key_owner_pid) do
    send(pid, {:key_owner_died, key_owner_pid})
  end

  ## ------------------------------------------------------------------
  ## OTP Process Functions
  ## ------------------------------------------------------------------

  # https://gist.github.com/marcelog/97708058cd17f86326c82970a7f81d40#file-simpleproc-erl

  def init(parent) do
    _ = Process.flag(:trap_exit, true)
    debug = :sys.debug_options([])
    Process.register(self(), @server)

    committed_epoch = 0
    state = state(committed_epoch: committed_epoch)
    :proc_lib.init_ack(parent, {:ok, self(), committed_epoch})
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

  defp loop(parent, debug, state) do
    receive do
      msg ->
        handle_msg(parent, debug, state, msg)
    end
  end

  defp handle_msg(parent, debug, state, msg) do
    case msg do
      {:release_key_lock, epoch, key_owner_pid, commitment} ->
        state = handle_key_lock_release(state, epoch, key_owner_pid, commitment)
        loop(parent, debug, state)
      {:keys_locked, epoch, lock_ref, key_owner_pids} ->
        state = handle_new_key_locks(state, epoch, lock_ref, key_owner_pids)
        loop(parent, debug, state)
      {:key_owner_died, _key_owner_pid} ->
        exit({:todo, :key_owner_died}) # TODO
      {:system, from, request} ->
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, state)
      {:"EXIT", pid, reason} ->
        state = deal_with_linked_process_death(parent, state, pid, reason)
        loop(parent, debug, state)
    end
  end

  ## ------------------------------------------------------------------
  ## Private Functions - Key Locks
  ## ------------------------------------------------------------------

  defp handle_new_key_locks(state, epoch, lock_ref, key_owner_pids) do
    state(committed_epoch: committed_epoch, queue: queue) = state

    case epoch > committed_epoch do
      true when not is_map_key(queue, epoch) ->
        in_progress = in_progress(lock_ref: lock_ref, locked: key_owner_pids)
        queue = Map.put(queue, epoch, in_progress)
        state(state, queue: queue)
      true when is_map_key(queue, epoch) ->
        exit({:repeated_epoch, :handle_new_key_locks, epoch})
      false ->
        exit({:outdated_epoch, :handle_new_key_locks, %{committed: committed_epoch, received: epoch}})
    end
  end

  defp handle_key_lock_release(state, epoch, key_owner_pid, key_owner_commitment) do
    state(committed_epoch: committed_epoch, queue: queue) = state

    case Map.get(queue, epoch) do
      in_progress(
        lock_ref: lock_ref, locked: locked, released: released,
        commit: commit
      ) = in_progress ->

        case Exdis.List.take!(locked, key_owner_pid) do
          [] when epoch === committed_epoch + 1 ->
            released = [key_owner_pid | released]
            commit = commit and (key_owner_commitment === :commit)
            reply_msg = {lock_ref, lock_release_reply(commit)}
            Enum.each(released, fn pid -> send(pid, reply_msg) end)
            queue = Map.delete(queue, epoch)
            {committed_epoch, queue} = maybe_commit_oldest_in_queue(epoch, queue)
            state(state, committed_epoch: committed_epoch, queue: queue)

          locked ->
            released = [key_owner_pid | released]
            commit = commit and (key_owner_commitment === :commit)
            in_progress = in_progress(in_progress, locked: locked, released: released, commit: commit)
            queue = Map.replace!(queue, epoch, in_progress)
            state(state, queue: queue)
        end

      nil ->
        exit({:epoch_not_found, :handle_key_lock_release, %{committed: committed_epoch, received: epoch}})
    end
  end

  defp maybe_commit_oldest_in_queue(committed_epoch, queue) do
    case Map.get(queue, committed_epoch + 1) do
      in_progress(lock_ref: lock_ref, locked: [], released: released, commit: commit) ->
        reply_msg = {lock_ref, lock_release_reply(commit)}
        Enum.each(released, fn pid -> send(pid, reply_msg) end)
        queue = Map.delete(queue, committed_epoch)
        maybe_commit_oldest_in_queue(committed_epoch + 1, queue)
      in_progress() ->
        {committed_epoch, queue}
      nil when map_size(queue) === 0 ->
        {committed_epoch, queue}
    end
  end

  defp lock_release_reply(commit) do
    case commit do
      true -> :committed
      false -> :discarded
    end
  end

  ## ------------------------------------------------------------------
  ## Private Functions - Death of Linked Processes
  ## ------------------------------------------------------------------

  defp deal_with_linked_process_death(parent, _state, pid, reason) when pid === parent do
    exit(reason)
  end
end
