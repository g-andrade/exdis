defmodule Exdis.DatabaseRegistry do
  use Bitwise
  use GenServer
  require Logger
  require Record

  ## ------------------------------------------------------------------
  ## Constant Definitions
  ## ------------------------------------------------------------------

  @server __MODULE__
  @persistent_term __MODULE__.Databases
  @number_of_databases 16

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  @type database :: %{key_registry_pid: pid, key_registry_table: :ets.tab}

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def start_link([]) do
    GenServer.start_link(__MODULE__, [], name: @server)
  end

  @spec get_database(non_neg_integer) :: database
  def get_database(database_index \\ 0) do
    %{} = all_registries = :persistent_term.get(@persistent_term)
    all_registries[database_index]
  end

  ## ------------------------------------------------------------------
  ## GenServer Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def init([]) do
    _ = Process.flag(:trap_exit, true)
    _ = start_databases()
    state = :no_state
    {:ok, state}
  end

  @impl true
  def handle_call(call, from, state) do
    {:stop, {:unexpected_call, from, call}, state}
  end

  @impl true
  def handle_cast(cast, state) do
    {:stop, {:unexpected_cast, cast}, state}
  end

  @impl true
  def handle_info({:"EXIT", pid, reason}, state) do
    handle_linked_process_death(pid, reason, state)
  end

  def handle_info(info, state) do
    {:stop, {:unexpected_info, info}, state}
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp start_databases() do
    database_indices = 0..(@number_of_databases - 1)
    persistent_term =
      Enum.reduce(database_indices, %{},
        fn database_index, acc ->
          {:ok, pid, table} = Exdis.Database.KeyRegistry.start_link()
          database = %{key_registry_pid: pid, key_registry_table: table}
          Map.put(acc, database_index, database)
        end)

    :persistent_term.put(@persistent_term, persistent_term)
  end

  defp handle_linked_process_death(pid, reason, state) do
    databases = :persistent_term.get(@persistent_term)
    database_list = Map.to_list(databases)

    case (for {idx, %{key_registry_pid: ^pid} = db} <- database_list, do: {idx, db}) do
      [{idx, db}] ->
        {:stop, {:database_stopped, %{index: idx, database: db, reason: reason}}, state}
      [] ->
        {:stop, {:linked_process_stopped, %{pid: pid, reason: reason}}, state}
    end
  end
end
