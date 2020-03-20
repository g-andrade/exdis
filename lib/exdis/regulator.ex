defmodule Exdis.Regulator do
  @behaviour :sregulator

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @server __MODULE__

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
    :sregulator.start_link({:local, @server}, __MODULE__, [], [])
  end

  def ask() do
    case :sregulator.ask(@server) do
      {:go, ref, pid, _, _} ->
        {:go, ref, pid}
      {:drop, _} ->
        :drop
    end
  end

  def done(pid, ref) do
    :sregulator.dirty_done(pid, ref)
  end

  ## ------------------------------------------------------------------
  ## :sregulator Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def init([]) do
	ask_queue_spec = ask_queue_spec()
    valve_spec = valve_spec()
    meter_specs = [overload_meter_spec()]
    {:ok, {ask_queue_spec, valve_spec, meter_specs}}
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp ask_queue_spec() do
    {:sbroker_drop_queue, %{}}
  end

  defp valve_spec() do
    {:sregulator_open_valve, %{max: 1}}
  end

  defp overload_meter_spec() do
    {:sbroker_overload_meter, %{alarm: {:overload, @server}}}
  end
end
