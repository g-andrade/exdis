defmodule Exdis.CommandParsers.String do
  ## ------------------------------------------------------------------
  ## APPEND Command
  ## ------------------------------------------------------------------

  def append([{:string, key}, {:string, tail}]) do
    {:ok, [key], &Exdis.Database.String.append(&1, key, tail)}
  end

  def append(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## DECR Command
  ## ------------------------------------------------------------------

  def decrement([{:string, key}]) do
    {:ok, [key], &Exdis.Database.String.increment_by(&1, key, -1)}
  end

  def decrement(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## DECRBY Command
  ## ------------------------------------------------------------------

  def decrement_by([{:string, key}, resp_decrement]) do
    case maybe_coerce_resp_value_into_int64(resp_decrement) do
      {:ok, decrement} ->
        {:ok, [key], &Exdis.Database.String.increment_by(&1, key, -decrement)}
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

  def get([{:string, key}]) do
    {:ok, [key], &Exdis.Database.String.get(&1, key)}
  end

  def get(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## GETBIT Command
  ## ------------------------------------------------------------------

  def get_bit([{:string, key}, resp_offset]) do
    case maybe_coerce_resp_value_into_int64(resp_offset) do
      {:ok, offset} when offset >= 0 ->
        {:ok, [key], &Exdis.Database.String.get_bit(&1, key, offset)}
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

  def get_range([{:string, key}, resp_start, resp_finish]) do
    case {maybe_coerce_resp_value_into_int64(resp_start),
          maybe_coerce_resp_value_into_int64(resp_finish)}
    do
      {{:ok, start}, {:ok, finish}} ->
        {:ok, [key], &Exdis.Database.String.get_range(&1, key, start, finish)}
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

  def get_set([{:string, key}, resp_value]) do
    case maybe_coerce_resp_value_into_string(resp_value) do
      {:ok, value} ->
        {:ok, [key], &Exdis.Database.String.get_set(&1, key, value)}
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

  def increment([{:string, key}]) do
    {:ok, [key], &Exdis.Database.String.increment_by(&1, key, +1)}
  end

  def increment(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## INCRBY Command
  ## ------------------------------------------------------------------

  def increment_by([{:string, key}, resp_increment]) do
    case maybe_coerce_resp_value_into_int64(resp_increment) do
      {:ok, increment} ->
        {:ok, [key], &Exdis.Database.String.increment_by(&1, key, +increment)}
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

  def increment_by_float([{:string, key}, resp_increment]) do
    case maybe_coerce_resp_value_into_float(resp_increment) do
      {:ok, increment} ->
        {:ok, [key], &Exdis.Database.String.increment_by_float(&1, key, +increment)}
      {:error, _} ->
        {:error, {:not_a_valid_float, "increment"}}
    end
  end

  def increment_by_float(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## SET Command
  ## ------------------------------------------------------------------

  def set([{:string, key}, resp_value]) do
    case maybe_coerce_resp_value_into_string(resp_value) do
      {:ok, value} ->
        {:ok, [key], &Exdis.Database.String.set(&1, key, value)}
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

  def str_length([{:string, key}]) do
    {:ok, [key], &Exdis.Database.String.str_length(&1, key)}
  end

  def str_length(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## RESP Type Coercion - To String
  ## ------------------------------------------------------------------

  defp maybe_coerce_resp_value_into_string({:string, string}) do
    {:ok, string}
  end

  defp maybe_coerce_resp_value_into_string({:integer, integer}) do
    {:ok, Exdis.Int64.to_decimal_string(integer)}
  end

  defp maybe_coerce_resp_value_into_string(_) do
    {:error, :unsupported_conversion}
  end

  ## ------------------------------------------------------------------
  ## RESP Type Coercion - To Integer
  ## ------------------------------------------------------------------

  defp maybe_coerce_resp_value_into_int64({:integer, integer}) do
    {:ok, Exdis.Int64.new(integer)}
  end

  defp maybe_coerce_resp_value_into_int64({:string, string}) do
    Exdis.Int64.from_decimal_string(string)
  end

  defp maybe_coerce_resp_value_into_int64(_) do
    {:error, :unsupported_conversion}
  end

  ## ------------------------------------------------------------------
  ## RESP Type Coercion - To Float
  ## ------------------------------------------------------------------

  defp maybe_coerce_resp_value_into_float({:string, string}) do
    Exdis.Float.from_decimal_string(string)
  end

  defp maybe_coerce_resp_value_into_float({:integer, integer}) do
    Exdis.Float.from_integer(integer)
  end

  defp maybe_coerce_resp_value_into_float(_) do
    {:error, :unsupported_conversion}
  end
end
