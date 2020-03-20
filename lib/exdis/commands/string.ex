defmodule Exdis.Commands.String do
  ## ------------------------------------------------------------------
  ## APPEND Command
  ## ------------------------------------------------------------------

  def append([{:string, key}, {:string, tail}], reply_cb) do
    Exdis.Database.String.append(key, tail, reply_cb)
  end

  def append(_, reply_cb) do
    reply_cb.(:sync, {:error, :bad_syntax})
  end

  ## ------------------------------------------------------------------
  ## DECR Command
  ## ------------------------------------------------------------------

  def decrement([{:string, key}], reply_cb) do
    Exdis.Database.String.increment_by(key, -1, reply_cb)
  end

  def decrement(_, reply_cb) do
    reply_cb.(:sync, {:error, :bad_syntax})
  end

  ## ------------------------------------------------------------------
  ## DECRBY Command
  ## ------------------------------------------------------------------

  def decrement_by([{:string, key}, resp_decrement], reply_cb) do
    case maybe_coerce_resp_value_into_integer(resp_decrement) do
      decrement when is_integer(decrement) ->
        Exdis.Database.String.increment_by(key, -decrement, reply_cb)
      :no ->
        reply_cb.(:sync, {:error, :value_not_an_integer_or_out_of_range})
    end
  end

  def decrement_by(_, reply_cb) do
    reply_cb.(:sync, {:error, :bad_syntax})
  end

  ## ------------------------------------------------------------------
  ## GET Command
  ## ------------------------------------------------------------------

  def get([{:string, key}], reply_cb) do
    Exdis.Database.String.get(key, reply_cb)
  end

  def get(_, reply_cb) do
    reply_cb.(:sync, {:error, :bad_syntax})
  end

  ## ------------------------------------------------------------------
  ## INCR Command
  ## ------------------------------------------------------------------

  def increment([{:string, key}], reply_cb) do
    Exdis.Database.String.increment_by(key, +1, reply_cb)
  end

  def increment(_, reply_cb) do
    reply_cb.(:sync, {:error, :bad_syntax})
  end

  ## ------------------------------------------------------------------
  ## INCRBY Command
  ## ------------------------------------------------------------------

  def increment_by([{:string, key}, resp_increment], reply_cb) do
    case maybe_coerce_resp_value_into_integer(resp_increment) do
      increment when is_integer(increment) ->
        Exdis.Database.String.increment_by(key, +increment, reply_cb)
      :no ->
        reply_cb.(:sync, {:error, :value_not_an_integer_or_out_of_range})
    end
  end

  def increment_by(_, reply_cb) do
    reply_cb.(:sync, {:error, :bad_syntax})
  end

  ## ------------------------------------------------------------------
  ## INCRBYFLOAT Command
  ## ------------------------------------------------------------------

  def increment_by_float([{:string, key}, resp_increment], reply_cb) do
    case maybe_coerce_resp_value_into_float(resp_increment) do
      increment when is_float(increment) ->
        Exdis.Database.String.increment_by_float(key, +increment, reply_cb)
      :no ->
        reply_cb.(:sync, {:error, :value_not_a_valid_float})
    end
  end

  def increment_by_float(_, reply_cb) do
    reply_cb.(:sync, {:error, :bad_syntax})
  end

  ## ------------------------------------------------------------------
  ## SET Command
  ## ------------------------------------------------------------------

  def set([{:string, key}, resp_value], reply_cb) do
    case maybe_coerce_resp_value_into_string(resp_value) do
      <<value :: bytes>> ->
        Exdis.Database.String.set(key, value, reply_cb)
      :no ->
        reply_cb.(:sync, {:error, :bad_syntax})
    end
  end

  def set(_, reply_cb) do
    reply_cb.(:sync, {:error, :bad_syntax})
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
    case Float.parse(string) do
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
