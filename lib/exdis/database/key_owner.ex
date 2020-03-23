defmodule Exdis.Database.KeyOwner do
  use GenServer
  require Record

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @gen_server_opts [hibernate_after: :timer.seconds(10)]

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:state,
    key: nil,
    registry_pid: nil,
    value_state: nil,
    lock: nil,
    pending_locks: nil,
    futures: nil
  )

  Record.defrecord(:lock,
    ref: nil,
    owner_pid: nil,
    owner_mon: nil,
    uncommitted_value_state: nil
  )

  Record.defrecord(:pending_locks,
    all: nil,
    owner_mons: nil,
    queue: nil,
    queue_index_counter: 0
  )

  Record.defrecord(:pending_lock,
    owner_pid: nil,
    owner_mon: nil,
    queue_index: nil
  )

  Record.defrecord(:future,
    owner_pid: nil,
    generate_cb: nil
  )

  ## ------------------------------------------------------------------
  ## High level API
  ## ------------------------------------------------------------------

  def manipulate(database, key, manipulation_cb) do
    {lock_ref, pid} = acquire_lock(database, key)
    manipulation_result = manipulate_value(pid, lock_ref, manipulation_cb)
    :ok = release_lock(pid, lock_ref)

    case manipulation_result do
      :ok ->
        Process.demonitor(lock_ref, [:flush])
        :ok
      {:ok, future_ref} ->
        mon = lock_ref
        {:async_ok, pid, mon, future_ref}
      {:error, _} = error ->
        Process.demonitor(lock_ref, [:flush])
        error
    end
  end

  ## ------------------------------------------------------------------
  ## Low Level API
  ## ------------------------------------------------------------------

  def acquire_lock(database, key, retries_left \\ 2) do
    case Exdis.Database.KeyRegistry.get_owner(database, key) do
      nil ->
        lock_ref_transfer_ref = make_ref()
        case start_and_acquire_lock(database, key, self(), lock_ref_transfer_ref) do
          {:ok, pid} ->
            lock_ref = Process.monitor(pid)
            send(pid, {lock_ref_transfer_ref , lock_ref})
            {lock_ref, pid}
          {:error, {:already_registered, _}} when retries_left > 0 ->
            acquire_lock(database, key, retries_left - 1)
          {:error, reason} ->
            exit({reason, {__MODULE__, :acquire_lock, [database, key, retries_left]}})
        end

      pid ->
        lock_ref = Process.monitor(pid)
        GenServer.cast(pid, {:acquire_lock, lock_ref, self()})
        receive do
          {^lock_ref, :ok} ->
            {lock_ref, pid}
          {:"DOWN", ^lock_ref, _, _, reason} ->
            case reason in [:noproc, :normal] and retries_left > 0 do
              true ->
                acquire_lock(database, key, retries_left - 1)
              false ->
                exit({reason, {__MODULE__, :acquire_lock, [database, key, retries_left]}})
            end
        end
    end
  end

  def manipulate_value(pid, lock_ref, manipulation_cb) do
    make_call_as_lock_owner(pid, lock_ref, &perform_value_manipulation(manipulation_cb, &1, &2))
  end

  def release_lock(pid, lock_ref, commitment \\ :commit) when commitment in [:commit, :discard] do
    make_call_as_lock_owner(pid, lock_ref, &perform_lock_release(commitment, &1, &2))
  end

  def consume_future(pid, mon, future_ref) do
    make_call_as_future_owner(pid, mon, future_ref, &perform_future_consumption/3)
  end

  ## ------------------------------------------------------------------
  ## proc_lib Function Definitions
  ## ------------------------------------------------------------------

  def proc_lib_init(database, key, lock_owner_pid, lock_ref_transfer_ref) do
    _ = Process.flag(:trap_exit, true)

    case Exdis.Database.KeyRegistry.register_owner(database, key) do
      {:ok, registry_pid} ->
        proc_lib_init_lock_ref(key, lock_owner_pid, lock_ref_transfer_ref, registry_pid)
      {:error, {:already_registered, _}} = error ->
        :proc_lib.init_ack(error)
    end
  end

  defp proc_lib_init_lock_ref(key, lock_owner_pid, lock_ref_transfer_ref, registry_pid) do
    lock_owner_mon = Process.monitor(lock_owner_pid)
    :proc_lib.init_ack({:ok, self()})

    receive do
      {^lock_ref_transfer_ref, lock_ref} ->
        lock = lock(ref: lock_ref, owner_pid: lock_owner_pid, owner_mon: lock_owner_mon)
        pending_locks = pending_locks(all: %{}, owner_mons: %{}, queue: :gb_trees.empty())
        state = state(
          key: key,
          registry_pid: registry_pid,
          lock: lock,
          pending_locks: pending_locks,
          futures: %{})

        :gen_server.enter_loop(__MODULE__, @gen_server_opts, state)

      {:"DOWN", ^lock_owner_mon, _, _, _} ->
        :ok = Exdis.Database.KeyRegistry.unregister_owner(registry_pid, key)
        exit(:normal)
    end
  end

  ## ------------------------------------------------------------------
  ## GenServer Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def init(_) do
    exit(:not_supposed_to_run)
  end

  @impl true
  def handle_call(call, from, state) do
    {:stop, {:unexpected_call, from, call}, state}
  end

  @impl true
  def handle_cast({:acquire_lock, lock_ref, owner_pid}, state) do
    handle_acquire_lock_request(lock_ref, owner_pid, state)
  end

  def handle_cast({:lock_owner_call, lock_ref, callback}, state) do
    handle_lock_owner_call(lock_ref, callback, state)
  end

  def handle_cast({:future_owner_call, future_ref, callback}, state) do
    handle_future_owner_call(future_ref, callback, state)
  end

  def handle_cast(cast, state) do
    {:stop, {:unexpected_cast, cast}, state}
  end

  @impl true
  def handle_info({:"DOWN", ref, _, _, _} = info, state) do
    case identify_monitor_ref(ref, state) do
      :lock_owner ->
        handle_lock_owner_death(state)
      {:pending_lock_owner, lock_ref} ->
        handle_pending_lock_owner_death(lock_ref, state)
      {:future_owner, ref} ->
        handle_future_owner_death(ref, state)
      :unknown ->
        {:stop, {:unexpected_info, info}, state}
    end
  end

  def handle_info({:"EXIT", pid, _}, state(registry_pid: pid) = state) do
    {:stop, :shutdown, state}
  end

  def handle_info(info, state) do
    {:stop, {:unexpected_info, info}, state}
  end

  @impl true
  def terminate(reason, state) when reason !== :shutdown do
    state(key: key, registry_pid: registry_pid) = state
    :ok = Exdis.Database.KeyRegistry.unregister_owner(registry_pid, key)
  end

  def terminate(_reason, _state), do: :ok


  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp start_and_acquire_lock(database, key, lock_owner_pid, lock_ref_transfer_ref) do
    init_args = [database, key, lock_owner_pid, lock_ref_transfer_ref]
    :proc_lib.start(__MODULE__, :proc_lib_init, init_args)
  end

  defp identify_monitor_ref(ref, state) do
    state(lock: lock) = state
    case lock do
      lock(owner_mon: ^ref) ->
        :lock_owner
      _ ->
        identify_monitor_ref_amongst_pending_locks(ref, state)
    end
  end

  defp identify_monitor_ref_amongst_pending_locks(ref, state) do
    state(pending_locks: pending_locks) = state
    pending_locks(owner_mons: owner_mons) = pending_locks

    try do
      lock_ref = Map.fetch!(owner_mons, ref)
      {:pending_lock_owner, lock_ref}
    rescue
      _ in KeyError ->
        identify_monitor_ref_amongst_futures(ref, state)
    end
  end

  defp identify_monitor_ref_amongst_futures(ref, state) do
    state(futures: futures) = state

    case Map.has_key?(futures, ref) do
      true ->
        {:future_owner, ref}
      false ->
        :unknown
    end
  end

  ## ------------------------------------------------------------------
  ## Active Lock
  ## ------------------------------------------------------------------

  defp handle_acquire_lock_request(ref, owner_pid, state(lock: lock) = state) do
    owner_mon = Process.monitor(owner_pid)
    case lock do
      nil ->
        state = grant_lock(ref, owner_pid, owner_mon, state)
        {:noreply, state}
      _ ->
        state = enqueue_pending_lock(ref, owner_pid, owner_mon, state)
        {:noreply, state}
    end
  end

  defp grant_lock(ref, owner_pid, owner_mon, state(lock: nil, value_state: value_state) = state) do
    send(owner_pid, {ref, :ok})
    new_lock = lock(
      ref: ref,
      owner_pid: owner_pid,
      owner_mon: owner_mon,
      uncommitted_value_state: value_state)

    state(state, lock: new_lock)
  end

  def perform_lock_release(commit, lock, state) do
    lock(ref: ref,
      owner_pid: owner_pid,
      owner_mon: owner_mon,
      uncommitted_value_state: uncommitted_value_state) = lock

    Process.demonitor(owner_mon, [:flush])
    send(owner_pid, {ref, :ok})
    state = state(state, lock: nil)
    state =
      case commit do
        :commit -> state(state, value_state: uncommitted_value_state)
        :discard -> state
      end

    state = maybe_grant_next_lock(state)
    # XXX stop here, if need be
    {:ok, state}
  end

  defp handle_lock_owner_death(state) do
    # XXX cancel transaction here, if need be
    # XXX stop here, if need be
    state = state(state, lock: nil)
    state = maybe_grant_next_lock(state)
    {:noreply, state}
  end

  defp make_call_as_lock_owner(pid, lock_ref, call_cb) do
    GenServer.cast(pid, {:lock_owner_call, lock_ref, call_cb})
    receive do
      {^lock_ref, result} ->
        result
      {:"DOWN", ^lock_ref, _, _, reason} ->
        exit({reason, {__MODULE__, :make_call_as_lock_owner, [pid, lock_ref, call_cb]}})
    end
  end

  defp handle_lock_owner_call(ref, callback, state) do
    state(lock: lock) = state
    case lock do
      lock(ref: ^ref, owner_pid: owner_pid) ->
        {result, state} = callback.(lock, state)
        send(owner_pid, {ref, result})
        {:noreply, state}
      _ ->
        {:stop, {:mismatched_lock_owner_call, ref, callback}, state}
    end
  end

  ## ------------------------------------------------------------------
  ## Pending Locks
  ## ------------------------------------------------------------------

  defp enqueue_pending_lock(ref, owner_pid, owner_mon, state) do
    state(pending_locks: pending_locks) = state
    pending_locks(
      all: all, owner_mons: owner_mons, queue: queue,
      queue_index_counter: queue_index_counter) = pending_locks

    new_pending_lock =
      pending_lock(
        owner_pid: owner_pid,
        owner_mon: owner_mon,
        queue_index: queue_index_counter
      )

    all = Map.put(all, ref, new_pending_lock)
    owner_mons = Map.put(owner_mons, owner_mon, ref)
    queue = :gb_trees.insert(queue_index_counter, ref, queue)
    queue_index_counter = queue_index_counter + 1

    pending_locks = pending_locks(pending_locks,
      all: all, owner_mons: owner_mons, queue: queue,
      queue_index_counter: queue_index_counter)

    state(state, pending_locks: pending_locks)
  end

  defp maybe_grant_next_lock(state(lock: lock, pending_locks: pending_locks) = state) do
    pending_locks(all: all, owner_mons: owner_mons, queue: queue) = pending_locks
    case lock === nil and map_size(all) !== 0 do
      true ->
        {queue_index, ref, queue} = :gb_trees.take_smallest(queue)
        {pending_lock, all} = Map.pop(all, ref)
        pending_lock(
          owner_pid: owner_pid,
          owner_mon: owner_mon,
          queue_index: ^queue_index
        ) = pending_lock

        {^ref, owner_mons} = Map.pop(owner_mons, owner_mon)
        pending_locks = pending_locks(pending_locks, all: all, owner_mons: owner_mons, queue: queue)
        state = state(state, pending_locks: pending_locks)
        grant_lock(ref, owner_pid, owner_mon, state)
      false ->
        state
    end
  end

  defp handle_pending_lock_owner_death(ref, state) do
    state(pending_locks: pending_locks) = state
    pending_locks(all: all, owner_mons: owner_mons, queue: queue) = pending_locks
    {pending_lock, all} = Map.pop(all, ref)
    pending_lock(owner_mon: owner_mon, queue_index: queue_index) = pending_lock

    {^ref, owner_mons} = Map.pop(owner_mons, owner_mon)
    {^ref, queue} = :gb_trees.take(queue_index, queue)
    pending_locks = pending_locks(pending_locks, all: all, owner_mons: owner_mons, queue: queue)
    state = state(state, pending_locks: pending_locks)
    # XXX stop there, if need be
    {:noreply, state}
  end

  ## ------------------------------------------------------------------
  ## Value Manipulation
  ## ------------------------------------------------------------------

  defp perform_value_manipulation(manipulation_cb, lock, state) do
    lock(owner_pid: owner_pid, uncommitted_value_state: value_state) = lock

    case manipulation_cb.(value_state) do
      :ok ->
        {:ok, state}

      {:ok, reply} ->
        future_cb = fn -> {:finished, reply} end # FIXME
        {future_ref, state} = add_future(owner_pid, future_cb, state)
        {{:ok, future_ref}, state}

      {:ok_and_update, value_state} ->
        lock = lock(lock, uncommitted_value_state: value_state)
        state = state(state, lock: lock)
        {:ok, state}

      {:ok_and_update, reply, value_state} ->
        future_cb = fn -> {:finished, reply} end # FIXME
        {future_ref, state} = add_future(owner_pid, future_cb, state)
        lock = lock(lock, uncommitted_value_state: value_state)
        state = state(state, lock: lock)
        {{:ok, future_ref}, state}

      {:error, reason} ->
        {{:error, reason}, state}

      {:error_and_update, reason, value_state} ->
        lock = lock(lock, uncommitted_value_state: value_state)
        state = state(state, lock: lock)
        {{:error, reason}, state}
    end
  end

  ## ------------------------------------------------------------------
  ## Reply Futures
  ## ------------------------------------------------------------------

  defp add_future(owner_pid, generate_cb, state) do
    state(futures: futures) = state
    ref = Process.monitor(owner_pid)
    future = future(owner_pid: owner_pid, generate_cb: generate_cb)
    futures = Map.put(futures, ref, future)
    state = state(state, futures: futures)
    {ref, state}
  end

  defp perform_future_consumption(ref, future, state) do
    state(futures: futures) = state
    future(generate_cb: generate_cb) = future

    case generate_cb.() do
      {:more, chunk, generate_cb} ->
        future = future(future, generate_cb: generate_cb)
        futures = Map.replace!(futures, ref, future)
        state = state(state, futures: futures)
        {{:more, chunk}, state}

      {:finished, chunk} ->
        Process.demonitor(ref, [:flush])
        futures = Map.delete(futures, ref)
        state = state(state, futures: futures)
        {{:finished, chunk}, state}
    end
  end

  defp handle_future_owner_death(ref, state) do
    state(futures: futures) = state
    {_future, futures} = Map.pop(futures, ref)
    state = state(state, futures: futures)
    # XXX stop there, if need be
    {:noreply, state}
  end

  defp make_call_as_future_owner(pid, mon, future_ref, call_cb) do
    GenServer.cast(pid, {:future_owner_call, future_ref, call_cb})
    receive do
      {^future_ref, result} ->
        result
      {:"DOWN", ^mon, _, _, reason} ->
        exit({reason, {__MODULE__, :make_call_as_future_owner, [pid, mon, future_ref, call_cb]}})
    end
  end

  defp handle_future_owner_call(ref, callback, state) do
    state(futures: futures) = state

    case Map.get(futures, ref) do
      future(owner_pid: owner_pid) = future ->
        {result, state} = callback.(ref, future, state)
        send(owner_pid, {ref, result})
        {:noreply, state}
      nil ->
        {:stop, {:mismatched_future_owner_call, ref, callback}, state}
    end
  end

end
