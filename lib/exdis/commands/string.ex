defmodule Exdis.Commands.String do
  ## ------------------------------------------------------------------
  ## APPEND Command
  ## ------------------------------------------------------------------

  def append([{:string, key}, {:string, tail}]) do
    Exdis.Database.String.append(key, tail)
  end

  def append(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## DECR Command
  ## ------------------------------------------------------------------

  def decrement([{:string, key}]) do
    Exdis.Database.String.increment_by(key, -1)
  end

  def decrement(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## DECRBY Command
  ## ------------------------------------------------------------------

  def decrement_by([{:string, key}, resp_decrement]) do
    case maybe_coerce_resp_value_into_integer(resp_decrement) do
      decrement when is_integer(decrement) ->
        Exdis.Database.String.increment_by(key, -decrement)
      :no ->
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
    Exdis.Database.String.get(key)
  end

  def get(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## GETBIT Command
  ## ------------------------------------------------------------------

  def get_bit([{:string, key}, resp_offset]) do
    case maybe_coerce_resp_value_into_integer(resp_offset) do
      offset when is_integer(offset) and offset >= 0 ->
        Exdis.Database.String.get_bit(key, offset)
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
    case {maybe_coerce_resp_value_into_integer(resp_start),
          maybe_coerce_resp_value_into_integer(resp_finish)}
    do
      {start, finish} when is_integer(start) and is_integer(finish) ->
        Exdis.Database.String.get_range(key, start, finish)
      {nil, _} ->
        {:error, {:not_an_integer_or_out_of_range, "start"}}
      {_, nil} ->
        {:error, {:not_an_integer_or_out_of_range, "end"}}
    end
  end

  def get_range(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## INCR Command
  ## ------------------------------------------------------------------

  def increment([{:string, key}]) do
    Exdis.Database.String.increment_by(key, +1)
  end

  def increment(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## INCRBY Command
  ## ------------------------------------------------------------------

  def increment_by([{:string, key}, resp_increment]) do
    case maybe_coerce_resp_value_into_integer(resp_increment) do
      increment when is_integer(increment) ->
        Exdis.Database.String.increment_by(key, +increment)
      :no ->
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
      increment when is_float(increment) ->
        Exdis.Database.String.increment_by_float(key, +increment)
      :no ->
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
      <<value :: bytes>> ->
        Exdis.Database.String.set(key, value)
      :no ->
        {:error, :bad_syntax}
    end
  end

  def set(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## RESP Type Coercion - To String
  ## ------------------------------------------------------------------

  defp maybe_coerce_resp_value_into_string({:string, string}) do
    string
  end

  defp maybe_coerce_resp_value_into_string({:integer, integer}) do
    Integer.to_string(integer)
  end

  defp maybe_coerce_resp_value_into_string(_) do
    :no
  end

  ## ------------------------------------------------------------------
  ## RESP Type Coercion - To Integer
  ## ------------------------------------------------------------------

  defp maybe_coerce_resp_value_into_integer({:integer, integer}) do
    integer
  end

  defp maybe_coerce_resp_value_into_integer({:string, string}) do
    min_value = Exdis.Database.String.min_integer_value()
    max_value = Exdis.Database.String.max_integer_value()
    case (
      byte_size(string) <= Exdis.Database.String.max_integer_value_str_length()
      and Integer.parse(string))
    do
      {integer, ""} when integer >= min_value and integer <= max_value ->
        integer
      _ ->
        :no
    end
  end

  defp maybe_coerce_resp_value_into_integer(_) do
    :no
  end

  ## ------------------------------------------------------------------
  ## RESP Type Coercion - To Float
  ## ------------------------------------------------------------------

  defp maybe_coerce_resp_value_into_float({:string, string}) do
    case (
      byte_size(string) <= Exdis.Database.String.max_float_value_str_length()
      and Float.parse(string))
    do
      {float, ""} ->
        float
      _ ->
        :no
    end
  end

  defp maybe_coerce_resp_value_into_float({:integer, integer}) do
    1.0 * integer
  end

  defp maybe_coerce_resp_value_into_float(_) do
    :no
  end
end
