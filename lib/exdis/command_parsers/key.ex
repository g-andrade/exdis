defmodule Exdis.CommandParsers.Key do
  ## ------------------------------------------------------------------
  ## DEL Command
  ## ------------------------------------------------------------------

  def delete(args) do
    case Exdis.CommandParsers.Util.parse_string_list(args, [:non_empty, :unique]) do
      {:ok, key_names} ->
        {:ok, key_names, &Exdis.Database.Key.delete(&1), [:varargs]}
      {:error, _} ->
        {:error, :bad_syntax}
    end
  end

  ## ------------------------------------------------------------------
  ## EXISTS Command
  ## ------------------------------------------------------------------

  def exist?(args) do
    case Exdis.CommandParsers.Util.parse_string_list(args, [:non_empty, :unstable]) do
      {:ok, key_names} ->
        {:ok, key_names, &Exdis.Database.Key.exist?(&1), [:varargs]}
      {:error, _} ->
        {:error, :bad_syntax}
    end
  end

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
