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
          members ->
            {:array, members}
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
      {:integer, integer} ->
        [@integer, Exdis.RESP.Integer.encode(integer)]
      {:array, list} ->
        [@array, Exdis.RESP.Array.encode(list)]
      nil ->
        [@array, Exdis.RESP.Array.encode(nil)]
      {:error, reason_iodata} ->
        [@error, Exdis.RESP.SimpleString.encode(reason_iodata)]
      {:partial, partial_value} ->
        encode_partial(partial_value)
    end
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp encode_partial(partial_value) do
    case partial_value do
      {:string_start, size, iodata} ->
        [@bulk_string, Exdis.RESP.BulkString.encode_start(size, iodata)]
      {:string_continue, iodata} ->
        Exdis.RESP.BulkString.encode_more(iodata)
      {:string_finish, iodata} ->
        Exdis.RESP.BulkString.encode_finish(iodata)

      {:array_start, size, members} ->
        [@array, Exdis.RESP.Array.encode_start(size, members)]
      {:array_continue, members} ->
        Exdis.RESP.Array.encode_more(members)
      {:array_finish, members} ->
        Exdis.RESP.Array.encode_finish(members)
    end
  end
end
