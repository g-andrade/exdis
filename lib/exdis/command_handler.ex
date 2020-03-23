defmodule Exdis.CommandHandler do
  require Record

  ## ------------------------------------------------------------------
  ## Record and Type Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:state,
    writer_pid: nil,
    database: nil,
    transaction: nil
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
    case Exdis.Command.recv(recv_fun) do
      {:ok, keys, handler} ->
        run_command_handler(keys, handler, state)
      :start_transaction ->
        exit(:todo)
      :discard_transaction ->
        exit(:todo)
      :commit_transaction ->
        exit(:todo)
      {:error, {:parse, reason}} ->
        # TODO discard transaction
        reason_iodata = error_reason_to_iodata(reason)
        dispatch_write(state, :direct, {:error, reason_iodata})
        state
    end
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp dispatch_write(state, type, args) do
    state(writer_pid: write_pid) = state
    Exdis.ConnectionWriter.dispatch(write_pid, type, args)
  end

  defp run_command_handler(keys, handler, state) do
    key_locks = acquire_key_locks(keys, state)
    case handler.(key_locks) do
      :ok ->
        release_key_locks(key_locks)
        dispatch_write(state, :direct, {:simple_string, "OK"})
        state
      {:ok_async, key_owner_pid, future_ref} ->
        release_key_locks(key_locks)
        dispatch_write(state, :future, {key_owner_pid, future_ref})
        state
      {:error, reason} ->
        release_key_locks(key_locks)
        reason_iodata = error_reason_to_iodata(reason)
        dispatch_write(state, :direct, {:error, reason_iodata})
        state
    end
  end

  defp acquire_key_locks(keys, state) do
    state(writer_pid: writer_pid, database: database) = state
    unique_keys = Enum.uniq(keys)

    Enum.reduce(unique_keys, %{},
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
    end
  end
end
