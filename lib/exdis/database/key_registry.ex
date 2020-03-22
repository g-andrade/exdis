defmodule Exdis.Database.KeyRegistry do
  use GenServer
  require Logger
  require Record

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @server __MODULE__
  @table @server

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def child_spec([]) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []}
    }
  end

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: @server)
  end

  def register_owner(key) do
    GenServer.call(@server, {:register_owner, key}, :infinity)
  end

  def unregister_owner(pid, key) do
    GenServer.call(pid, {:unregister_owner, key}, :infinity)
  end

  def get_owner(key) do
    case :ets.lookup(@table, key) do
      [{_, owner_pid, _}] ->
        owner_pid
      [] ->
        nil
    end
  end

  def mark_as_set(key) do
    :ets.update_element(@table, key, {3, :set})
  end

  def mark_as_unset(key) do
    :ets.update_element(@table, key, {3, :unset})
  end

  ## ------------------------------------------------------------------
  ## :gen_server Function Definitions
  ## ------------------------------------------------------------------

  def init([]) do
    _ = Process.flag(:trap_exit, true)
    _ = create_table()
    state = :no_state
    {:ok, state}
  end

  def handle_call({:register_owner, key}, {owner_pid, _}, state) do
    handle_owner_registration(key, owner_pid, state)
  end

  def handle_call({:unregister_owner, key}, {alleged_owner_pid, _}, state) do
    handle_owner_unregistration(key, alleged_owner_pid, state)
  end

  def handle_call(call, from, state) do
    {:stop, {:unexpected_call, from, call}, state}
  end

  def handle_cast(cast, state) do
    {:stop, {:unexpected_cast, cast}, state}
  end

  def handle_info({:"EXIT", pid, _}, state) do
    handle_linked_process_death(pid, state)
  end

  def handle_info(info, state) do
    {:stop, {:unexpected_info, info}, state}
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  def create_table() do
    opts = [:named_table, :public, read_concurrency: true]
    :ets.new(@table, opts)
  end

  def handle_owner_registration(key, owner_pid, state) do
    case :ets.insert_new(@table, {key, owner_pid, :unset}) do
      true ->
        Process.link(owner_pid)
        {:reply, {:ok, self()}, state}
      false ->
        [{_, existing_owner_pid, _}] = :ets.lookup(@table, key)
        {:reply, {:already_registered, existing_owner_pid}, state}
    end
  end

  def handle_owner_unregistration(key, alleged_owner_pid, state) do
    case :ets.take(@table, key) do
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

  def handle_linked_process_death(pid, state) do
    # the following lookup can become terribly slow but it will only run for edge cases
    match_spec = [{{:"$1",:"$2",:_}, [{:"=:=",:"$2",pid}], [:"$1"]}]
    case :ets.select(@table, match_spec) do
      [key] ->
        Logger.warn("Owner of key #{inspect key} stopped before unregistering")
        :ets.delete(@table, key)
        {:noreply, state}
      [] ->
        Logger.warn("Linked process #{pid} died (should it be linked?)")
        {:noreply, state}
    end
  end
end
