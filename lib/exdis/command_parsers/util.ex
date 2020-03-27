defmodule Exdis.CommandParsers.Util do
  ## ------------------------------------------------------------------
  ## RESP Type Coercion - To String
  ## ------------------------------------------------------------------

  def maybe_coerce_into_string({:string, string}) do
    {:ok, string}
  end

  def maybe_coerce_into_string({:integer, integer}) do
    {:ok, Exdis.Int64.to_decimal_string(integer)}
  end

  def maybe_coerce_into_string(_) do
    {:error, :unsupported_conversion}
  end

  ## ------------------------------------------------------------------
  ## RESP Type Coercion - To Integer
  ## ------------------------------------------------------------------

  def maybe_coerce_into_int64({:integer, integer}) do
    {:ok, Exdis.Int64.new(integer)}
  end

  def maybe_coerce_into_int64({:string, string}) do
    Exdis.Int64.from_decimal_string(string)
  end

  def maybe_coerce_into_int64(_) do
    {:error, :unsupported_conversion}
  end

  ## ------------------------------------------------------------------
  ## RESP Type Coercion - To Float
  ## ------------------------------------------------------------------

  def maybe_coerce_into_float({:string, string}) do
    Exdis.Float.from_decimal_string(string)
  end

  def maybe_coerce_into_float({:integer, integer}) do
    Exdis.Float.from_integer(integer)
  end

  def maybe_coerce_into_float(_) do
    {:error, :unsupported_conversion}
  end

  ## ------------------------------------------------------------------
  ## Variadic Argument Helpers: Parsing String Lists
  ## ------------------------------------------------------------------

  def parse_string_list(list, opts \\ []) do
    parse_string_list_recur(list, opts, [])
  end

  defp parse_string_list_recur([{:string, string} | next], opts, acc) do
    acc = [string | acc]
    parse_string_list_recur(next, opts, acc)
  end

  defp parse_string_list_recur([], opts, acc) do
    cond do
      (:non_empty in opts) and (acc === []) ->
        {:error, :empty_list}
      :unique in opts ->
        {:ok, Enum.uniq(acc)}
      :unstable in opts ->
        {:ok, acc}
      true ->
        {:ok, Enum.reverse(acc)}
    end
  end

  ## ------------------------------------------------------------------
  ## Variadic Argument Helpers: Parsing Key-Value Lists
  ## ------------------------------------------------------------------

  def parse_and_unzip_kvlist(list, opts \\ []) do
    parse_and_unzip_kvlist_recur(list, opts, [], [])
  end

  defp parse_and_unzip_kvlist_recur(
    [{:string, key_name}, resp_value | next], opts, key_names_acc, values_acc)
  do
    case maybe_coerce_into_string(resp_value) do
      {:ok, value} ->
        key_names_acc = [key_name | key_names_acc]
        values_acc = [value | values_acc]
        parse_and_unzip_kvlist_recur(next, opts, key_names_acc, values_acc)
      {:error, reason} ->
        {:error, {:value_not_string, %{value: resp_value, reason: reason}}}
    end
  end

  defp parse_and_unzip_kvlist_recur([], opts, key_names_acc, values_acc) do
    cond do
      (:non_empty in opts) and (key_names_acc === []) ->
        {:error, :empty_list}
      :unique in opts ->
        pairs = Enum.zip(key_names_acc, values_acc)
        unique_pairs = :lists.ukeysort(1, pairs)
        {keys, values} = Enum.unzip(unique_pairs)
        {:ok, keys, values}
      :unstable in opts ->
        {:ok, key_names_acc, values_acc}
      true ->
        key_names = Enum.reverse(key_names_acc)
        values = Enum.reverse(values_acc)
        {:ok, key_names, values}
    end
  end

  defp parse_and_unzip_kvlist_recur([unpaired_entry], _opts, _key_names_acc, _values_acc) do
    {:error, {:unpaired_entry, unpaired_entry}}
  end
end
