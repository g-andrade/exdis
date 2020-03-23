defmodule Exdis.RESP.BulkString do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def recv(fun) do
    case Exdis.RESP.Integer.recv(fun) do
      size when size >= 0 ->
        line = fun.(size + 2)
        <<string :: bytes-size(size), "\r\n">> = line
        Exdis.RESP.Util.maybe_copy_line_subbinary(string, size)
    end
  end

  def encode(iodata) do
    size = :erlang.iolist_size(iodata)
    [
      Exdis.RESP.Integer.encode(size),
      iodata, ?\r, ?\n
    ]
  end
end
