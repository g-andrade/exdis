defmodule Exdis.RESP.Array do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def recv(fun) do
    case Exdis.RESP.Integer.recv(fun) do
      count when count >= 0 ->
        recv_elements(count, fun, [])
      0 ->
        nil
    end
  end

  def encode(list) when is_list(list) do
    size = length(list)
    encoded_size = Exdis.RESP.Integer.encode(size)
    encoded_elements = Enum.map(list, &Exdis.RESP.Value.encode/1)
    [encoded_size | encoded_elements]
  end

  def encode(nil) do
    # special case
    encoded_size = Exdis.RESP.Integer.encode(-1)
    encoded_size
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp recv_elements(count, fun, acc) do
    cond do
      count > 0 ->
        value = Exdis.RESP.Value.recv(fun)
        acc = [value | acc]
        recv_elements(count - 1, fun, acc)
      count === 0 ->
        Enum.reverse(acc)
    end
  end
end
