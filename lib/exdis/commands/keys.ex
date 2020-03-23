defmodule Exdis.Commands.Keys do
  ## ------------------------------------------------------------------
  ## KEYS Command
  ## ------------------------------------------------------------------

  def keys(database, [{:string, pattern}]) when pattern === "*" do
    # TODO support glob patterns
    resp_keys =
      Exdis.Database.KeyRegistry.reduce_keys(database, [],
        fn key ->
          [{:string, key}]
        end)
    {:ok, {:array, resp_keys}}
  end

  def keys(_, _) do
    {:error, :bad_syntax}
  end

end
