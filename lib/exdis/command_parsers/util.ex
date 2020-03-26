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
end
