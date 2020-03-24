defmodule Exdis.CommandHandler do
  require Logger
  require Record

  ## ------------------------------------------------------------------
  ## Record and Type Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:state,
    writer_pid: nil,
    database: nil,
    transaction: nil
  )

  Record.defrecord(:transaction,
    keys_to_lock: MapSet.new(),
    queue: [], # accumulated in reverse order
    errored: false
  )

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def new(writer_pid) do
    state(
      writer_pid: writer_pid,
      database: Exdis.DatabaseRegistry.get_database())
  end

  def receive_and_handle(state, recv_fun) do
    reception_result = Exdis.Command.recv(recv_fun)
    handle_reception_result(state, reception_result)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Handling Command Reception
  ## ------------------------------------------------------------------

  defp handle_reception_result(state, {:ok, keys, handler}) do
    state(transaction: transaction) = state
    case transaction do
      nil ->
        key_locks = acquire_key_locks(state, keys)
        run_command_handler(state, key_locks, handler)
        release_key_locks(key_locks)
        state
      transaction(keys_to_lock: keys_to_lock, queue: queue) ->
        keys_to_lock = MapSet.union(keys_to_lock, MapSet.new(keys))
        queue = [handler | queue]
        transaction = transaction(transaction, keys_to_lock: keys_to_lock, queue: queue)
        dispatch_direct_write(state, :queued)
        state(state, transaction: transaction)
    end
  end

  defp handle_reception_result(state, :start_transaction) do
    state(transaction: transaction) = state
    case transaction do
      nil ->
        dispatch_direct_write(state, :ok)
        state(state, transaction: transaction())
      transaction() ->
        dispatch_direct_write(state, {:error, {:calls_cannot_be_nested, "MULTI"}})
        state
    end
  end

  defp handle_reception_result(state, :execute_transaction) do
    state(transaction: transaction) = state
    case transaction do
      transaction(errored: false, keys_to_lock: keys_to_lock, queue: queue) ->
        execute_transaction(state, keys_to_lock, queue)
      transaction(errored: true) ->
        dispatch_direct_write(state, {:error, :transaction_discarded_because_of_previous_errors})
        state(state, transaction: nil)
      nil ->
        dispatch_direct_write(state, {:error, {:command_without_another_first, "EXEC", "MULTI"}})
        state
    end
  end

  defp handle_reception_result(state, :discard_transaction) do
    state(transaction: transaction) = state
    case transaction do
      transaction() ->
        dispatch_direct_write(state, :ok)
        state(state, transaction: nil)
      nil ->
        dispatch_direct_write(state, {:error, {:command_without_another_first, "DISCARD", "MULTI"}})
        state
    end
  end

  defp handle_reception_result(state, {:error, {:parse, reason}}) do
    state(transaction: transaction) = state
    case transaction do
      nil ->
        dispatch_direct_write(state, {:error, reason})
        state
      transaction() ->
        dispatch_direct_write(state, {:error, reason})
        transaction = transaction(transaction, errored: true)
        state(state, transaction: transaction)
    end
  end

  defp dispatch_direct_write(state, args) do
    state(writer_pid: write_pid) = state
    Exdis.ConnectionWriter.dispatch(write_pid, :direct, args)
  end

  defp dispatch_future_write(state, args) do
    state(writer_pid: write_pid) = state
    Exdis.ConnectionWriter.dispatch(write_pid, :future, args)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Executing Transactions
  ## ------------------------------------------------------------------

  defp execute_transaction(state, keys_to_lock, queue) do
    Logger.info("Executing transaction for keys #{inspect keys_to_lock} and queue #{inspect queue}")
    key_locks = acquire_key_locks(state, keys_to_lock)
    command_handlers = Enum.reverse(queue)
    Enum.each(command_handlers, &run_command_handler(state, key_locks, &1))
    release_key_locks(key_locks)
    state(state, transaction: nil)
  end

  defp run_command_handler(state, key_locks, handler) do
    case handler.(key_locks) do
      :ok ->
        dispatch_direct_write(state, :ok)
      {:ok_async, key_owner_pid, future_ref} ->
        dispatch_future_write(state, {key_owner_pid, future_ref})
      {:error, reason} ->
        dispatch_direct_write(state, {:error, reason})
    end
  end

  defp acquire_key_locks(state, keys) do
    state(writer_pid: writer_pid, database: database) = state
    Enum.reduce(keys, %{},
      fn key, acc ->
        {pid, lock_ref} = Exdis.Database.KeyOwner.acquire_lock(database, key, writer_pid)
        Map.put(acc, key, {pid, lock_ref})
      end)
  end

  defp release_key_locks(locks) do
    Enum.each(locks,
      fn {_key, {pid, lock_ref}} ->
        :ok = Exdis.Database.KeyOwner.release_lock(pid, lock_ref)
        Process.demonitor(lock_ref)
      end)
  end
end
