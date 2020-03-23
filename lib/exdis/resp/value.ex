defmodule Exdis.RESP.Value do
  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @simple_string ?+
  @error ?-
  @integer ?:
  @bulk_string ?$
  @array ?*

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def recv(fun) do
    case fun.(1) do
      <<@simple_string>> ->
        string = Exdis.RESP.SimpleString.recv(fun)
        {:string, string}
      <<@bulk_string>> ->
        string = Exdis.RESP.BulkString.recv(fun)
        {:string, string}
      <<@array>> ->
        case Exdis.RESP.Array.recv(fun) do
          nil ->
            nil
          elements ->
            {:array, elements}
        end
      <<@error>> ->
        reason_string = Exdis.RESP.SimpleString.recv(fun)
        {:error, reason_string}
      <<@integer>> ->
        integer = Exdis.RESP.Integer.recv(fun)
        {:integer, integer}
    end
  end

  def encode(value) do
    case value do
      {:simple_string, iodata} ->
        [@simple_string, Exdis.RESP.SimpleString.encode(iodata)]
      {:string, iodata} ->
        [@bulk_string, Exdis.RESP.BulkString.encode(iodata)]
      {:array, list} ->
        [@array, Exdis.RESP.Array.encode(list)]
      nil ->
        [@array, Exdis.RESP.Array.encode(nil)]
      {:error, reason_iodata} ->
        encoded = Exdis.RESP.SimpleString.encode(reason_iodata)
        [@error, encoded]
      {:integer, integer} ->
        [@integer, Exdis.RESP.Integer.encode(integer)]
    end
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------
end
