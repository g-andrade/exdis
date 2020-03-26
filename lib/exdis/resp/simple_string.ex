defmodule Exdis.RESP.SimpleString do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def recv(fun) do
    line = fun.(:line)
    case byte_size(line) - 2 do
      string_size when string_size >= 0 ->
        <<string :: bytes-size(string_size), "\r\n">> = line
        Exdis.RESP.Util.maybe_copy_line_subbinary(string, string_size)
    end
  end

  def encode(iodata) do
    string = :erlang.iolist_to_binary(iodata)
    :nomatch = :binary.match(string, ["\r", "\n"])
    [string, ?\r, ?\n]
  end
end
