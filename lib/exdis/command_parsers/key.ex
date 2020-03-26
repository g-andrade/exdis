defmodule Exdis.CommandParsers.Key do
  ## ------------------------------------------------------------------
  ## DEL Command
  ## ------------------------------------------------------------------

  def delete([_|_] = args) do
    delete_recur(args, [])
  end

  def delete([]) do
    {:error, :bad_syntax}
  end

  defp delete_recur([{:string, key_name} | next_args], key_names_acc) do
    key_names_acc = [key_name | key_names_acc]
    delete_recur(next_args, key_names_acc)
  end

  defp delete_recur([], key_names_acc) do
    {:ok, key_names_acc, &Exdis.Database.Key.delete(&1), [:varargs]}
  end

  defp delete_recur([_|_], _) do
    {:error, :bad_syntax}
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
