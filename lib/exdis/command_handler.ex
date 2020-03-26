defmodule Exdis.CommandHandler do
  require Logger
  require Record

  ## ------------------------------------------------------------------
  ## Record and Type Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:state,
    connection_writer_pid: nil,
    database_pid: nil,
    database_monitor: nil,
    database_index: nil,
    transaction: nil
  )

  Record.defrecord(:transaction,
    database_index: nil,
    keys_to_lock: MapSet.new(),
    queue: [], # accumulated in reverse order
    errored: false
  )

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def new(connection_writer_pid) do
    {:ok, database_pid, database_monitor} = Exdis.Database.pid_and_monitor()
    state(
      connection_writer_pid: connection_writer_pid,
      database_pid: database_pid,
      database_monitor: database_monitor,
      database_index: 0) # TODO
  end

  def receive_and_handle(state, recv_fun) do
    reception_result = Exdis.Command.recv(recv_fun)
    handle_reception_result(state, reception_result)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Handling Command Reception
  ## ------------------------------------------------------------------

  defp handle_reception_result(state, {:ok, command_key_names, command_handler_fun}) do
    handle_reception_result(state, {:ok, command_key_names, command_handler_fun, []})
  end

  defp handle_reception_result(state, {:ok, command_key_names, command_handler_fun, opts}) do
    state(transaction: transaction) = state

    case transaction do
      nil ->
        state(database_index: database_index) = state
        command_keys = for name <- command_key_names, do: {database_index, name}
        state(connection_writer_pid: connection_writer_pid) = state
        unique_command_keys = Enum.uniq(command_keys)
        {:ok, key_locks} = acquire_key_locks(state, unique_command_keys, connection_writer_pid)
        replies = run_command_handler(key_locks, command_keys, command_handler_fun, opts)
        :ok = release_key_locks(key_locks)
        Exdis.ConnectionWriter.dispatch(connection_writer_pid, replies)
        state
      transaction(keys_to_lock: trx_keys_to_lock, queue: queue) ->
        state(database_index: database_index) = state
        command_keys = for name <- command_key_names, do: {database_index, name}
        trx_keys_to_lock = MapSet.union(trx_keys_to_lock, MapSet.new(command_keys))
        queue = [{command_keys, command_handler_fun, opts} | queue]
        transaction = transaction(transaction, keys_to_lock: trx_keys_to_lock, queue: queue)
        dispatch_reply(state, :queued)
        state(state, transaction: transaction)
    end
  end

  defp handle_reception_result(state, :start_transaction) do
    state(transaction: transaction, database_index: database_index) = state
    case transaction do
      nil ->
        dispatch_reply(state, :ok)
        state(state, transaction: transaction(database_index: database_index))
      transaction() ->
        dispatch_reply(state, {:error, {:calls_cannot_be_nested, "MULTI"}})
        state
    end
  end

  defp handle_reception_result(state, :execute_transaction) do
    state(transaction: transaction) = state
    case transaction do
      transaction(errored: false, keys_to_lock: keys_to_lock, queue: queue, database_index: database_index) ->
        execute_transaction(state, keys_to_lock, queue, database_index)
      transaction(errored: true) ->
        dispatch_reply(state, {:error, :transaction_discarded_because_of_previous_errors})
        state(state, transaction: nil)
      nil ->
        dispatch_reply(state, {:error, {:command_without_another_first, "EXEC", "MULTI"}})
        state
    end
  end

  defp handle_reception_result(state, :discard_transaction) do
    state(transaction: transaction) = state
    case transaction do
      transaction() ->
        dispatch_reply(state, :ok)
        state(state, transaction: nil)
      nil ->
        dispatch_reply(state, {:error, {:command_without_another_first, "DISCARD", "MULTI"}})
        state
    end
  end

  defp handle_reception_result(state, {:error, {:parse, reason}}) do
    state(transaction: transaction) = state
    case transaction do
      nil ->
        dispatch_reply(state, {:error, reason})
        state
      transaction() ->
        dispatch_reply(state, {:error, reason})
        transaction = transaction(transaction, errored: true)
        state(state, transaction: transaction)
    end
  end

  defp dispatch_reply(state, reply) do
    state(connection_writer_pid: connection_writer_pid) = state
    Exdis.ConnectionWriter.dispatch(connection_writer_pid, [reply])
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions - Executing Transactions
  ## ------------------------------------------------------------------

  defp execute_transaction(state, keys_to_lock, queue, database_index) do
    state(connection_writer_pid: connection_writer_pid) = state
    keys_to_lock = Enum.to_list(keys_to_lock)
    Logger.info("Executing transaction for keys #{inspect keys_to_lock} and queue #{inspect queue}")

    {:ok, all_key_locks} = acquire_key_locks(state, keys_to_lock, connection_writer_pid)
    command_handlers = Enum.reverse(queue)
    commands_replies =
      Enum.map(command_handlers,
        fn ({command_keys, handler_fun, opts}) ->
          run_command_handler(all_key_locks, command_keys, handler_fun, opts)
        end)
    :ok = release_key_locks(all_key_locks)

    reply_array_start = {:partial, {:array_start, length(commands_replies), []}}
    reply_array_finish = {:partial, {:array_finish, []}}
    Exdis.ConnectionWriter.dispatch(connection_writer_pid, [reply_array_start])
    Enum.each(commands_replies,
      fn command_replies ->
        Exdis.ConnectionWriter.dispatch(connection_writer_pid, command_replies)
      end)
    Exdis.ConnectionWriter.dispatch(connection_writer_pid, [reply_array_finish])

    state(state, transaction: nil, database_index: database_index)
  end

  defp run_command_handler(all_key_locks, keys, handler_fun, opts) do
    %{owners: all_key_owners} = all_key_locks
    handler_args = for key <- keys, do: Map.fetch!(all_key_owners, key)
    case apply_command_handler_args(handler_fun, handler_args, opts) do
      :ok ->
        [:ok]
      {:ok, reply} ->
        [reply]
      {:error, _} = error ->
        [error]
      {:success_array, replies} ->
        reply_array_start = {:partial, {:array_start, length(replies), []}}
        reply_array_members = replies
        reply_array_finish = {:partial, {:array_finish, []}}
        [reply_array_start | reply_array_members] ++ [reply_array_finish]
    end
  end

  defp apply_command_handler_args(handler_fun, handler_args, opts) do
    case :use_varargs in opts do
      false -> apply(handler_fun, handler_args)
      true -> handler_fun.(handler_args)
    end
  end

  defp acquire_key_locks(state, keys, streams_reader) do
    state(database_pid: database_pid, database_monitor: database_monitor) = state
    lock_ref = database_monitor
    keys = Enum.to_list(keys)
    key_owner_pids = Exdis.Database.async_lock_keys(database_pid, keys, lock_ref, keys, streams_reader)
    key_owners_list =
      :lists.zipwith(
        fn key, pid -> {key, {pid, Process.monitor(pid)}} end,
        keys, key_owner_pids)

    key_owners = :maps.from_list(key_owners_list)
    {:ok, %{ref: lock_ref, owners: key_owners}}
  end

  defp release_key_locks(key_locks) do
    %{ref: lock_ref, owners: key_owners} = key_locks
    key_owner_pids_and_monitors = Map.values(key_owners)
    {:ok, :committed} = Exdis.Database.KeyOwner.release_locks(key_owner_pids_and_monitors, lock_ref)
    Enum.each(key_owner_pids_and_monitors,
      fn {_pid, monitor} -> Process.demonitor(monitor, [:flush]) end)
    :ok
  end
end
