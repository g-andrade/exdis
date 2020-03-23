defmodule Exdis.Database.KeyRegistry do
  use GenServer
  require Logger
  require Record

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:state,
    parent_pid: nil,
    table: nil
  )

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def start_link() do
    :proc_lib.start_link(__MODULE__, :proc_lib_init, [self()])
  end

  def get_owner(database, key) do
    %{key_registry_table: table} = database
    case :ets.lookup(table, key) do
      [{_, owner_pid, _}] ->
        owner_pid
      [] ->
        nil
    end
  end

  def register_owner(database, key) do
    %{key_registry_pid: pid} = database
    GenServer.call(pid, {:register_owner, key}, :infinity)
  end

  def unregister_owner(pid, key) do
    GenServer.call(pid, {:unregister_owner, key}, :infinity)
  end

  def for_each_key(database, fun) do
    %{key_registry_table: table} = database
    :ets.foldl(
      fn {key, _pid, _set}, _ ->
        fun.(key)
        :ok
      end,
      :ok, table)
  end

  def reduce_keys(database, acc, fun) do
    %{key_registry_table: table} = database
    :ets.foldl(
      fn {key, _pid, _set}, acc ->
        fun.(key, acc)
      end,
      acc, table)
  end

#  def mark_as_set(database, key) do
#    :ets.update_element(key, {3, :set})
#  end
#
#  def mark_as_unset(database, key) do
#    :ets.update_element(key, {3, :unset})
#  end

  ## ------------------------------------------------------------------
  ## :proc_lib Function Definitions
  ## ------------------------------------------------------------------

  def proc_lib_init(parent_pid) do
    _ = Process.flag(:trap_exit, true)
    table = create_table()
    state = state(parent_pid: parent_pid, table: table)

    :proc_lib.init_ack({:ok, self(), table})
    :gen_server.enter_loop(__MODULE__, [], state)
  end

  ## ------------------------------------------------------------------
  ## GenServer Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def init(_) do
    exit(:not_supposed_to_run)
  end

  @impl true
  def handle_call({:register_owner, key}, {owner_pid, _}, state) do
    handle_owner_registration(key, owner_pid, state)
  end

  def handle_call({:unregister_owner, key}, {alleged_owner_pid, _}, state) do
    handle_owner_unregistration(key, alleged_owner_pid, state)
  end

  def handle_call(call, from, state) do
    {:stop, {:unexpected_call, from, call}, state}
  end

  @impl true
  def handle_cast(cast, state) do
    {:stop, {:unexpected_cast, cast}, state}
  end

  @impl true
  def handle_info({:"EXIT", pid, _}, state) do
    handle_linked_process_death(pid, state)
  end

  def handle_info(info, state) do
    {:stop, {:unexpected_info, info}, state}
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp create_table() do
    opts = [:public, read_concurrency: true]
    :ets.new(__MODULE__, opts)
  end

  defp handle_owner_registration(key, owner_pid, state) do
    state(table: table) = state
    case :ets.insert_new(table, {key, owner_pid, :unset}) do
      true ->
        Process.link(owner_pid)
        {:reply, {:ok, self()}, state}
      false ->
        [{_, existing_owner_pid, _}] = :ets.lookup(table, key)
        {:reply, {:already_registered, existing_owner_pid}, state}
    end
  end

  defp handle_owner_unregistration(key, alleged_owner_pid, state) do
    state(table: table) = state
    case :ets.take(table, key) do
      [{_, ^alleged_owner_pid, _}] ->
        Process.unlink(alleged_owner_pid)
        {:reply, :ok, state}
      [{_, actual_owner_pid, _}] ->
        error_reason = {:mismatched_owner_pid, actual_owner_pid}
        {:reply, {:error, error_reason}, state}
      [] ->
        error_reason = :not_found
        {:reply, {:error, error_reason}, state}
    end
  end

  defp handle_linked_process_death(pid, state(parent_pid: pid) = state) do
    {:stop, :normal, state}
  end
end
