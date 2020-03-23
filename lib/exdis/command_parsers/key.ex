defmodule Exdis.CommandParsers.Key do
  ## ------------------------------------------------------------------
  ## KEYS Command
  ## ------------------------------------------------------------------

  def keys([{:string, pattern}]) when pattern === "*" do
    # TODO support glob patterns
    exit(:todo)
    #resp_keys =
    #  Exdis.Database.KeyRegistry.reduce_keys(database, [],
    #    fn key ->
    #      [{:string, key}]
    #    end)
    #{:ok, {:array, resp_keys}}
  end

  def keys(_) do
    {:error, :bad_syntax}
  end

end
