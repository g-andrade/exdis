defmodule Exdis.CommandParsers.String do
  ## ------------------------------------------------------------------
  ## APPEND Command
  ## ------------------------------------------------------------------

  def append([{:string, key_name}, {:string, tail}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.append(&1, tail)}
  end

  def append(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## DECR Command
  ## ------------------------------------------------------------------

  def decrement([{:string, key_name}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.increment_by(&1, -1)}
  end

  def decrement(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## DECRBY Command
  ## ------------------------------------------------------------------

  def decrement_by([{:string, key_name}, resp_decrement]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_decrement) do
      {:ok, decrement} ->
        {:ok, [key_name], &Exdis.Database.Value.String.increment_by(&1, -decrement)}
      {:error, _} ->
        {:error, {:not_an_integer_or_out_of_range, "decrement"}}
    end
  end

  def decrement_by(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## GET Command
  ## ------------------------------------------------------------------

  def get([{:string, key_name}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.get(&1)}
  end

  def get(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## GETBIT Command
  ## ------------------------------------------------------------------

  def get_bit([{:string, key_name}, resp_offset]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_offset) do
      {:ok, offset} when offset >= 0 ->
        {:ok, [key_name], &Exdis.Database.Value.String.get_bit(&1, offset)}
      _ ->
        {:error, {:not_an_integer_or_out_of_range, "bit offset"}}
    end
  end

  def get_bit(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## GETRANGE Command
  ## ------------------------------------------------------------------

  def get_range([{:string, key_name}, resp_start, resp_finish]) do
    case {Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_start),
          Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_finish)}
    do
      {{:ok, start}, {:ok, finish}} ->
        {:ok, [key_name], &Exdis.Database.Value.String.get_range(&1, start, finish)}
      {{:error, _}, _} ->
        {:error, {:not_an_integer_or_out_of_range, "start"}}
      {_, {:error, _}} ->
        {:error, {:not_an_integer_or_out_of_range, "end"}}
    end
  end

  def get_range(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## GETSET Command
  ## ------------------------------------------------------------------

  def get_set([{:string, key_name}, resp_value]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_string(resp_value) do
      {:ok, value} ->
        {:ok, [key_name], &Exdis.Database.Value.String.get_set(&1, value)}
      {:error, _} ->
        {:error, :bad_syntax}
    end
  end

  def get_set(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## INCR Command
  ## ------------------------------------------------------------------

  def increment([{:string, key_name}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.increment_by(&1, +1)}
  end

  def increment(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## INCRBY Command
  ## ------------------------------------------------------------------

  def increment_by([{:string, key_name}, resp_increment]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_increment) do
      {:ok, increment} ->
        {:ok, [key_name], &Exdis.Database.Value.String.increment_by(&1, +increment)}
      {:error, _} ->
        {:error, {:not_an_integer_or_out_of_range, "increment"}}
    end
  end

  def increment_by(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## INCRBYFLOAT Command
  ## ------------------------------------------------------------------

  def increment_by_float([{:string, key_name}, resp_increment]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_float(resp_increment) do
      {:ok, increment} ->
        {:ok, [key_name], &Exdis.Database.Value.String.increment_by_float(&1, +increment)}
      {:error, _} ->
        {:error, {:not_a_valid_float, "increment"}}
    end
  end

  def increment_by_float(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## MGET Command
  ## ------------------------------------------------------------------

  def mget([_|_] = args) do
    mget_recur(args, [])
  end

  def mget(_) do
    {:error, :bad_syntax}
  end

  defp mget_recur([{:string, key_name} | next_args], key_names_acc) do
    key_names_acc = [key_name | key_names_acc]
    mget_recur(next_args, key_names_acc)
  end

  defp mget_recur([], key_names_acc) do
    key_names = Enum.reverse(key_names_acc)
    {:ok, key_names, &Exdis.Database.Value.String.mget(&1), [:varargs]}
  end

  defp mget_recur([_|_], _) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## MSET Command
  ## ------------------------------------------------------------------

  def mset([_|_] = args) do
    mset_recur(args, [])
  end

  def mset(_) do
    {:error, :bad_syntax}
  end

  defp mset_recur([{:string, key_name}, resp_value | next_args], pairs_acc) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_string(resp_value) do
      {:ok, value} ->
        pairs_acc = [{key_name, value} | pairs_acc]
        mset_recur(next_args, pairs_acc)
      {:error, _} ->
        {:error, :bad_syntax}
    end
  end

  defp mset_recur([], pairs_acc) do
    {key_names, values} = Enum.unzip(pairs_acc)
    {:ok, key_names, &Exdis.Database.Value.String.mset(&1, values), [:varargs]}
  end

  defp mset_recur([_|_], _) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## SET Command
  ## ------------------------------------------------------------------

  def set([{:string, key_name}, resp_value]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_string(resp_value) do
      {:ok, value} ->
        {:ok, [key_name], &Exdis.Database.Value.String.set(&1, value)}
      {:error, _} ->
        {:error, :bad_syntax}
    end
  end

  def set(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## STRLEN Command
  ## ------------------------------------------------------------------

  def str_length([{:string, key_name}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.str_length(&1)}
  end

  def str_length(_) do
    {:error, :bad_syntax}
  end
end
