defmodule Exdis.RESP.Array do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def recv(fun) do
    case Exdis.RESP.Integer.recv(fun) do
      count when count >= 0 ->
        recv_members(count, fun, [])
      0 ->
        nil
    end
  end

  def encode(list) when is_list(list) do
    size = length(list)
    [encode_start(size, list), encode_finish([])]
  end

  def encode(nil) do
    # special case
    encoded_size = Exdis.RESP.Integer.encode(-1)
    encoded_size
  end

  def encode_start(size, members) do
    encoded_size = Exdis.RESP.Integer.encode(size)
    encoded_members = Enum.map(members, &Exdis.RESP.Value.encode/1)
    [encoded_size, encoded_members]
  end

  def encode_more(members) do
    Enum.map(members, &Exdis.RESP.Value.encode/1)
  end

  def encode_finish(members) do
    Enum.map(members, &Exdis.RESP.Value.encode/1)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp recv_members(count, fun, acc) do
    cond do
      count > 0 ->
        value = Exdis.RESP.Value.recv(fun)
        acc = [value | acc]
        recv_members(count - 1, fun, acc)
      count === 0 ->
        Enum.reverse(acc)
    end
  end
end
